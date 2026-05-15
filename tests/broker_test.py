"""
Broker integration test harness for autocontrol.

Tests the full broker message flow without physical hardware:

  - autocontrol is started in-process (background threads, no Streamlit)
  - FakeDevice stubs run in asyncio and respond to command.<device_id>.submit_task
    by publishing task.completed after a configurable simulated delay
  - BrokerTestClient sends commands to command.autocontrol.# and collects
    scheduler.* events, enabling assertion-based test scenarios

Requires:
  RabbitMQ running at localhost:5672
    docker-compose up -d rabbitmq
  roadmap-broker-client installed in this venv
    pip install -e ../roadmap-broker-client

Usage:
  cd autocontrol
  python tests/broker_test.py
  # or with verbose broker logging:
  BROKER_TEST_VERBOSE=1 python tests/broker_test.py
"""

import asyncio
import json
import logging
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path

import aio_pika

# Allow running from the repo root or from autocontrol/
sys.path.insert(0, str(Path(__file__).parent.parent))

from roadmap_broker_client.connection import get_connection
from roadmap_broker_client.envelope import Envelope, build
from roadmap_broker_client.publisher import publish
from roadmap_broker_client.topology import (
    DLX_EXCHANGE,
    declare_topology,
)
from roadmap_broker_client.topics import (
    INSTRUMENT_EXCHANGE,
    SCHEDULER_TASK_COMPLETED,
    SCHEDULER_TASK_DISPATCHED,
    SCHEDULER_TASK_FAILED,
    SCHEDULER_QUEUE_UPDATED,
    TASK_COMPLETED,
    TASK_FAILED,
    command_key,
    command_subscription_pattern,
)

import autocontrol.server as server
from autocontrol.task_struct import Task, TaskData, TaskType

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

_verbose = bool(os.getenv("BROKER_TEST_VERBOSE"))
logging.basicConfig(
    level=logging.DEBUG if _verbose else logging.WARNING,
    format="%(asctime)s  %(name)-20s  %(levelname)s  %(message)s",
)
log = logging.getLogger("broker_test")
log.setLevel(logging.DEBUG)

# ---------------------------------------------------------------------------
# Timing constants
# ---------------------------------------------------------------------------

FAKE_DEVICE_DELAY = 0.3   # seconds a fake device waits before reporting completion
EVENT_TIMEOUT    = 10.0   # seconds to wait for an expected broker event
ATC_START_WAIT   = 3.0    # seconds to allow the broker worker to connect on startup

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
_results: list[tuple[str, bool]] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    status = PASS if condition else FAIL
    suffix = f"  ({detail})" if detail and not condition else ""
    print(f"  [{status}] {name}{suffix}")
    _results.append((name, condition))


# ---------------------------------------------------------------------------
# FakeDevice — acts as a broker-speaking device stub
# ---------------------------------------------------------------------------

class FakeDevice:
    """Subscribes to command.<device_id>.# and replies with task.completed."""

    def __init__(self, device_id: str, delay: float = FAKE_DEVICE_DELAY) -> None:
        self.device_id = device_id
        self.delay = delay
        self.commands_received: list[Envelope] = []
        self._exchange: aio_pika.abc.AbstractExchange | None = None

    async def run(self, channel: aio_pika.abc.AbstractChannel) -> None:
        self._exchange = await channel.get_exchange(INSTRUMENT_EXCHANGE)

        queue = await channel.declare_queue(
            f"test.fake_device.{self.device_id}",
            durable=False,
            auto_delete=True,
            arguments={"x-dead-letter-exchange": DLX_EXCHANGE},
        )
        await queue.bind(
            self._exchange,
            routing_key=command_subscription_pattern(self.device_id),
        )
        log.debug("[FakeDevice %s] ready", self.device_id)

        async with queue.iterator() as messages:
            async for message in messages:
                try:
                    envelope = Envelope.model_validate_json(message.body)
                    await message.ack()
                    self.commands_received.append(envelope)
                    asyncio.create_task(self._respond(envelope))
                except Exception:
                    log.exception("[FakeDevice %s] error handling message", self.device_id)
                    await message.nack(requeue=False)

    async def _respond(self, envelope: Envelope) -> None:
        task_type = envelope.payload.get("task_type", "?")
        log.debug(
            "[FakeDevice %s] handling %s task_id=%s — sleeping %.1fs",
            self.device_id, task_type, envelope.task_id, self.delay,
        )
        await asyncio.sleep(self.delay)

        response = build(
            device_id=self.device_id,
            routing_key=TASK_COMPLETED,
            task_id=envelope.task_id,
            sample_id=envelope.sample_id,
            assigned_channel=envelope.assigned_channel,
            execution_policy=envelope.execution_policy,
            payload={"device": self.device_id, "task_type": task_type},
        )
        await publish(self._exchange, TASK_COMPLETED, response)
        log.debug("[FakeDevice %s] published task.completed for task_id=%s", self.device_id, envelope.task_id)


# ---------------------------------------------------------------------------
# BrokerTestClient — sends commands and collects scheduler events
# ---------------------------------------------------------------------------

class BrokerTestClient:
    """
    Publishes commands to command.autocontrol.# and collects all
    scheduler.* and task.* events.  Use wait_for_event() to block until
    a specific routing key arrives, with a timeout.
    """

    def __init__(self) -> None:
        self._exchange: aio_pika.abc.AbstractExchange | None = None
        # List of (routing_key, envelope) received
        self._received: list[tuple[str, Envelope]] = []
        self._condition = asyncio.Condition()

    async def setup(self, channel: aio_pika.abc.AbstractChannel) -> None:
        self._exchange = await channel.get_exchange(INSTRUMENT_EXCHANGE)

        queue = await channel.declare_queue(
            "test.client.events",
            durable=False,
            auto_delete=True,
        )
        await queue.bind(self._exchange, routing_key="scheduler.#")
        await queue.bind(self._exchange, routing_key="task.#")

        asyncio.create_task(self._consume(queue))

    async def _consume(self, queue: aio_pika.abc.AbstractQueue) -> None:
        async with queue.iterator() as messages:
            async for message in messages:
                try:
                    envelope = Envelope.model_validate_json(message.body)
                    rk = message.routing_key or ""
                    log.debug("[TestClient] event: %s  task_id=%s", rk, envelope.task_id)
                    async with self._condition:
                        self._received.append((rk, envelope))
                        self._condition.notify_all()
                    await message.ack()
                except Exception:
                    log.exception("[TestClient] error processing event")
                    await message.nack(requeue=False)

    async def wait_for_event(
        self,
        routing_key: str,
        task_id: uuid.UUID | None = None,
        timeout: float = EVENT_TIMEOUT,
    ) -> tuple[str, Envelope] | tuple[None, None]:
        """
        Wait until an event matching routing_key (exact or prefix) arrives.
        Optionally filter by task_id.  Returns (routing_key, envelope) or (None, None).
        """
        deadline = asyncio.get_event_loop().time() + timeout

        def _matches(rk: str, env: Envelope) -> bool:
            key_ok = rk == routing_key or rk.startswith(routing_key.rstrip("#").rstrip("."))
            id_ok = task_id is None or env.task_id == task_id
            return key_ok and id_ok

        async with self._condition:
            while True:
                for rk, env in self._received:
                    if _matches(rk, env):
                        return rk, env

                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    return None, None
                try:
                    await asyncio.wait_for(
                        self._condition.wait(), timeout=remaining
                    )
                except asyncio.TimeoutError:
                    return None, None

    def clear(self) -> None:
        self._received.clear()

    async def send_command(self, verb: str, payload: dict) -> None:
        rk = command_key("autocontrol", verb)
        envelope = build(device_id="test_client", routing_key=rk, payload=payload)
        await publish(self._exchange, rk, envelope)
        log.debug("[TestClient] sent command.autocontrol.%s", verb)

    async def submit_task(self, task: Task) -> uuid.UUID:
        """Submit a task to autocontrol via broker and return its task_id."""
        # task.json() works on both Pydantic v1 and v2
        payload = json.loads(task.json())
        await self.send_command("submit_task", payload)
        return task.id


# ---------------------------------------------------------------------------
# Task builders
# ---------------------------------------------------------------------------

def make_init(device: str, device_type: str = "lh", channels: int = 4) -> Task:
    return Task(
        task_type=TaskType.INIT,
        tasks=[TaskData(
            device=device,
            device_type=device_type,
            device_address=f"http://localhost:9999/{device}",
            number_of_channels=channels,
            simulated=False,
            sample_mixing=True,
            md={"description": f"{device} init"},
        )],
    )


def make_prepare(device: str, sample_id: uuid.UUID) -> Task:
    return Task(
        sample_id=sample_id,
        task_type=TaskType.PREPARE,
        tasks=[TaskData(
            device=device,
            method_data={"step": "prepare"},
            md={"description": f"prepare on {device}"},
        )],
    )


def make_transfer(src: str, dst: str, sample_id: uuid.UUID) -> Task:
    return Task(
        sample_id=sample_id,
        task_type=TaskType.TRANSFER,
        tasks=[
            TaskData(device=src, method_data={"step": "transfer_src"}),
            TaskData(device=dst, method_data={"step": "transfer_dst"}),
        ],
    )


def make_measure(device: str, sample_id: uuid.UUID) -> Task:
    return Task(
        sample_id=sample_id,
        task_type=TaskType.MEASURE,
        tasks=[TaskData(
            device=device,
            method_data={"step": "measure"},
            acquisition_time=1.0,
            md={"description": f"measure on {device}"},
        )],
    )


# ---------------------------------------------------------------------------
# Test scenarios
# ---------------------------------------------------------------------------

async def test_init(client: BrokerTestClient, atc) -> None:
    print("\n[test_init] Submit INIT task via broker → expect scheduler.task_completed")
    client.clear()

    task = make_init("lh1", "lh", channels=4)
    await client.submit_task(task)

    rk, env = await client.wait_for_event(SCHEDULER_QUEUE_UPDATED)
    check("queue_updated received after submit", env is not None)

    rk, env = await client.wait_for_event(SCHEDULER_TASK_COMPLETED, task_id=task.id)
    check("task_completed received for INIT", env is not None)

    check("lh1 registered in atc.devices", "lh1" in atc.devices)
    check("lh1 has 4 channels in channel_po", len(atc.channel_po.get("lh1", [])) == 4)


async def test_prepare_roundtrip(client: BrokerTestClient, atc) -> None:
    print("\n[test_prepare_roundtrip] Submit PREPARE → fake device responds → scheduler events arrive")
    client.clear()
    sample_id = uuid.uuid4()

    task = make_prepare("lh1", sample_id)
    await client.submit_task(task)

    rk, env = await client.wait_for_event(SCHEDULER_TASK_DISPATCHED, task_id=task.id)
    check("task_dispatched published", env is not None)
    check("dispatched envelope has correct device", env.payload.get("device") == "lh1" if env else False)

    rk, env = await client.wait_for_event(SCHEDULER_TASK_COMPLETED, task_id=task.id)
    check("task_completed published after device responds", env is not None)

    # Post-process should have moved task to history
    history_task = atc.sample_history.get_task_by_id(str(task.id))
    check("task moved to sample_history after completion", history_task is not None)


async def test_channel_affinity(client: BrokerTestClient, atc) -> None:
    print("\n[test_channel_affinity] PREPARE on lh1 then TRANSFER to qcmd1 → same channel on qcmd1")
    client.clear()

    # Init qcmd1 first (lh1 was inited in test_init)
    init_task = make_init("qcmd1", "qcmd", channels=2)
    await client.submit_task(init_task)
    _, env = await client.wait_for_event(SCHEDULER_TASK_COMPLETED, task_id=init_task.id)
    check("qcmd1 INIT completed", env is not None)
    client.clear()

    sample_id = uuid.uuid4()

    prep = make_prepare("lh1", sample_id)
    await client.submit_task(prep)
    _, env = await client.wait_for_event(SCHEDULER_TASK_COMPLETED, task_id=prep.id)
    check("PREPARE completed", env is not None)

    # Discover which channel was assigned
    history_prep = atc.sample_history.get_task_by_id(str(prep.id))
    lh1_channel = history_prep.tasks[0].channel if history_prep else None
    check("PREPARE has an assigned channel", lh1_channel is not None)
    client.clear()

    xfer = make_transfer("lh1", "qcmd1", sample_id)
    await client.submit_task(xfer)
    _, env = await client.wait_for_event(SCHEDULER_TASK_COMPLETED, task_id=xfer.id)
    check("TRANSFER completed", env is not None)

    history_xfer = atc.sample_history.get_task_by_id(str(xfer.id))
    xfer_src_channel = history_xfer.tasks[0].channel if history_xfer else None
    check("TRANSFER source channel matches PREPARE channel", xfer_src_channel == lh1_channel)


async def test_cancel(client: BrokerTestClient, atc) -> None:
    print("\n[test_cancel] Pause scheduler, submit task, cancel it, verify it disappears from queue")
    client.clear()

    # Pause so the task sits in the queue
    await client.send_command("pause", {})
    await asyncio.sleep(0.2)

    sample_id = uuid.uuid4()
    task = make_prepare("lh1", sample_id)
    await client.submit_task(task)

    _, env = await client.wait_for_event(SCHEDULER_QUEUE_UPDATED, timeout=5)
    check("queue_updated after submit (paused)", env is not None)

    queued = atc.queue.get_task_by_id(str(task.id))
    check("task is in priority queue", queued is not None)

    await client.send_command("cancel_task", {"task_id": str(task.id)})
    await asyncio.sleep(0.3)

    check("task removed from queue after cancel", atc.queue.get_task_by_id(str(task.id)) is None)

    # Resume for subsequent tests
    await client.send_command("resume", {})
    await asyncio.sleep(0.2)


async def test_pause_resume(client: BrokerTestClient, atc) -> None:
    print("\n[test_pause_resume] Pause → submit task → verify no dispatch → resume → verify dispatch")
    client.clear()

    await client.send_command("pause", {})
    await asyncio.sleep(0.2)
    check("scheduler paused", atc.paused is True)

    sample_id = uuid.uuid4()
    task = make_prepare("lh1", sample_id)
    await client.submit_task(task)
    await asyncio.sleep(0.5)

    check("no dispatch while paused", atc.active_tasks.get_task_by_id(str(task.id)) is None)

    await client.send_command("resume", {})
    check("scheduler resumed", atc.paused is False)

    _, env = await client.wait_for_event(SCHEDULER_TASK_COMPLETED, task_id=task.id)
    check("task completes after resume", env is not None)


# ---------------------------------------------------------------------------
# Main harness
# ---------------------------------------------------------------------------

async def run_harness() -> None:
    # Start autocontrol directly (no Streamlit)
    storage_path = tempfile.mkdtemp(prefix="atc_broker_test_")
    log.debug("Using storage path: %s", storage_path)

    print("Starting autocontrol server ...")
    server.start_server(hostname="localhost", port=5099, storage_path=storage_path)

    print(f"Waiting {ATC_START_WAIT}s for broker worker to connect ...")
    await asyncio.sleep(ATC_START_WAIT)

    atc = server.atc  # direct reference for assertions

    # Connect as test infrastructure (client + device stubs)
    connection = await get_connection()

    async with connection:
        ch_client  = await connection.channel()
        ch_dev_lh  = await connection.channel()
        ch_dev_q   = await connection.channel()

        await ch_client.set_qos(prefetch_count=10)
        await ch_dev_lh.set_qos(prefetch_count=1)
        await ch_dev_q.set_qos(prefetch_count=1)

        # Declare topology so our queues bind successfully
        await declare_topology(ch_client)

        client = BrokerTestClient()
        await client.setup(ch_client)

        fake_lh1  = FakeDevice("lh1")
        fake_qcmd1 = FakeDevice("qcmd1")

        # Start device stubs as background tasks
        asyncio.create_task(fake_lh1.run(ch_dev_lh))
        asyncio.create_task(fake_qcmd1.run(ch_dev_q))

        # Brief pause to let stubs bind their queues
        await asyncio.sleep(0.5)

        print("\n" + "=" * 60)
        print("Running broker tests")
        print("=" * 60)

        try:
            await test_init(client, atc)
            await test_prepare_roundtrip(client, atc)
            await test_channel_affinity(client, atc)
            await test_cancel(client, atc)
            await test_pause_resume(client, atc)
        except Exception:
            log.exception("Unhandled exception in test run")

    # Summary
    print("\n" + "=" * 60)
    passed = sum(1 for _, ok in _results if ok)
    total  = len(_results)
    print(f"Results: {passed}/{total} passed")
    if passed < total:
        print("Failed tests:")
        for name, ok in _results:
            if not ok:
                print(f"  - {name}")
    print("=" * 60)

    # Clean up storage
    import shutil
    shutil.rmtree(storage_path, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(run_harness())
