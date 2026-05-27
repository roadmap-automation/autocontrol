"""Broker integration for autocontrol — central hardware scheduler.

Runs an asyncio event loop in a dedicated daemon thread alongside the
synchronous Flask server.  Provides:

  Inbound (broker → autocontrol):
    - command.autocontrol.# on exchange.instrument
      verbs: submit_task, cancel_task, resubmit_task, pause, resume, reset
    - task.completed / task.failed on exchange.instrument
      (device completion events that drive post_process_task)

  Outbound (autocontrol → broker):
    - scheduler.task_dispatched   after a task is sent to a device
    - scheduler.task_completed    after post_process_task succeeds
    - scheduler.task_failed       after a device failure is processed
    - scheduler.channel_locked    when channel_po transitions to occupied
    - scheduler.channel_released  when channel_po transitions to free
    - scheduler.queue_updated     when the priority queue changes
    - command.<device_id>.submit_task  replaces POST /SubmitTask to devices

  Audit log:
    Every state transition is written to the task_events PostgreSQL table.

Thread model
------------
The atc instance and Flask background loop run in normal threads.
The broker consumer runs in an asyncio event loop on a dedicated daemon
thread.  Completion events from devices are passed back to the Flask
background loop via a thread-safe queue.Queue so atc state is never
mutated from two threads simultaneously.
"""

import asyncio
import logging
import os
import queue
import threading
from typing import Optional

import aio_pika
from pydantic import ValidationError

from roadmap_broker_client.connection import get_connection
from roadmap_broker_client.consumer import consume
from roadmap_broker_client.envelope import Envelope, build
from roadmap_broker_client.publisher import publish
from roadmap_broker_client.topology import (
    declare_event_queue,
    declare_node_queue,
    declare_topology,
)
from roadmap_broker_client.topics import (
    CMD_SUBMIT_TASK,
    DEVICE_REGISTERED,
    INSTRUMENT_EXCHANGE,
    SCHEDULER_CHANNEL_LOCKED,
    SCHEDULER_CHANNEL_RELEASED,
    SCHEDULER_QUEUE_UPDATED,
    SCHEDULER_TASK_COMPLETED,
    SCHEDULER_TASK_DISPATCHED,
    SCHEDULER_TASK_FAILED,
    TASK_COMPLETED,
    TASK_FAILED,
    command_key,
)

from autocontrol.task_struct import Task, TaskType

logger = logging.getLogger(__name__)

# Execution policy derived from TaskType (SYSTEM_REQUIREMENTS.md §4)
_EXECUTION_POLICY: dict[TaskType, str] = {
    TaskType.PREPARE:   "irreversible",
    TaskType.TRANSFER:  "uncertain",
    TaskType.MEASURE:   "repeatable",
    TaskType.INIT:      "infrastructure",
    TaskType.SHUTDOWN:  "infrastructure",
    TaskType.NOCHANNEL: "infrastructure",
    TaskType.NONE:      "infrastructure",
}


class BrokerWorker:
    """Owns the async event loop, all AMQP objects, and the PostgreSQL audit log.

    Exposes a thread-safe API for the synchronous Flask background loop:

      completion_events  — queue.Queue of (task_id_str, status, envelope)
                           populated by device task.completed / task.failed

      dispatch_to_device(task, subtask)
          Publish command.<device_id>.submit_task to the device's command
          queue.  Call after process_task() returns True.

      publish_dispatched(task, subtask)
          Publish scheduler.task_dispatched and write the audit log row.

      publish_task_completed(task)
          Publish scheduler.task_completed and write the audit log row.

      publish_task_failed(task, error)
          Publish scheduler.task_failed and write the audit log row.

      publish_channel_locked(device, channel)
      publish_channel_released(device, channel)
          Publish channel occupancy transitions.

      publish_queue_updated()
          Lightweight event; no payload required.
    """

    def __init__(self, atc_instance) -> None:
        self.atc = atc_instance
        # Completion events from async consumer → sync dispatch loop
        self.completion_events: queue.Queue = queue.Queue()
        # Set whenever new work arrives so background_task() wakes immediately
        self.wakeup: threading.Event = threading.Event()

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._instrument_exchange: Optional[aio_pika.abc.AbstractExchange] = None
        self._pg_pool = None  # asyncpg pool, set in _run()
        self._thread: Optional[threading.Thread] = None

        # Subtask completion tracking for multi-device TRANSFER tasks.
        # Maps task_id (str) → number of subtasks still outstanding.
        # Incremented in task_dispatch_hook() (sync thread) once per dispatched
        # subtask.  Decremented in _on_device_event() (async thread) on each
        # task.completed.  The completion event is only forwarded to
        # completion_events when the count reaches zero.
        self._pending_subtasks: dict[str, int] = {}
        self._pending_subtasks_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Thread-safe public API  (called from the sync Flask thread)
    # ------------------------------------------------------------------

    def dispatch_to_device(self, task: Task, subtask) -> None:
        """Publish command.<device_id>.submit_task to the device's queue."""
        self._schedule(self._publish_device_command(task, subtask))

    def publish_dispatched(self, task: Task, subtask) -> None:
        """Publish scheduler.task_dispatched and log to audit table."""
        self._schedule(self._emit_dispatched(task, subtask))

    def publish_task_completed(self, task: Task, device_payload: dict = None) -> None:
        extra = dict(device_payload) if device_payload else {}
        self._schedule(self._emit_scheduler_event(SCHEDULER_TASK_COMPLETED, task, extra))

    def publish_task_failed(self, task: Task, error: str) -> None:
        policy = _EXECUTION_POLICY.get(task.task_type, "infrastructure")
        self._schedule(self._emit_scheduler_event(
            SCHEDULER_TASK_FAILED, task, {"error": error, "error_domain": policy}
        ))

    def publish_channel_locked(self, device: str, channel: int) -> None:
        self._schedule(self._emit_channel_event(SCHEDULER_CHANNEL_LOCKED, device, channel))

    def publish_channel_released(self, device: str, channel: int) -> None:
        self._schedule(self._emit_channel_event(SCHEDULER_CHANNEL_RELEASED, device, channel))

    def publish_queue_updated(self) -> None:
        self._schedule(self._emit_lightweight(SCHEDULER_QUEUE_UPDATED))

    def task_dispatch_hook(self, task: Task, subtask) -> None:
        """Callback wired to atc.task_dispatch_hook.

        Called by process_task() after each subtask is successfully accepted
        by a device.  Publishes both the device command and the scheduler
        dispatched event.

        INIT tasks auto-complete here: hardware init happens at device service
        startup, not on broker command. Not all device types have broker
        consumers, so we cannot rely on task.completed arriving from the device.
        """
        if task.task_type == TaskType.INIT:
            self.completion_events.put((str(task.id), "completed", None))
            self.wakeup.set()
            return
        with self._pending_subtasks_lock:
            task_id_str = str(task.id)
            self._pending_subtasks[task_id_str] = self._pending_subtasks.get(task_id_str, 0) + 1
        self.dispatch_to_device(task, subtask)
        self.publish_dispatched(task, subtask)

    # ------------------------------------------------------------------
    # Internal: schedule a coroutine on the broker event loop
    # ------------------------------------------------------------------

    def _schedule(self, coro) -> None:
        if self._loop is None or self._loop.is_closed():
            logger.warning("Broker loop not running; dropping coroutine.")
            return
        asyncio.run_coroutine_threadsafe(coro, self._loop)

    # ------------------------------------------------------------------
    # Async publish helpers
    # ------------------------------------------------------------------

    async def _emit_dispatched(self, task: Task, subtask) -> None:
        if self._instrument_exchange is None:
            return
        policy = _EXECUTION_POLICY.get(task.task_type, "infrastructure")
        envelope = build(
            device_id="autocontrol",
            routing_key=SCHEDULER_TASK_DISPATCHED,
            task_id=task.id,
            sample_id=task.sample_id,
            assigned_channel=subtask.channel,
            execution_policy=policy,
            payload={
                "device": subtask.device,
                "channel": subtask.channel,
                "task_type": task.task_type.value,
            },
        )
        await publish(self._instrument_exchange, SCHEDULER_TASK_DISPATCHED, envelope)
        await self._log_event(
            task_id=task.id,
            sample_id=task.sample_id,
            sample_number=task.sample_number,
            event_type="dispatched",
            task_type=task.task_type.value,
            execution_policy=policy,
            device_id=subtask.device,
            channel_index=subtask.channel,
            event_data={"subtask_id": str(subtask.id)},
        )

    async def _emit_scheduler_event(
        self, routing_key: str, task: Task, extra: dict
    ) -> None:
        if self._instrument_exchange is None:
            return
        policy = _EXECUTION_POLICY.get(task.task_type, "infrastructure")
        subtask = task.tasks[0] if task.tasks else None
        envelope = build(
            device_id="autocontrol",
            routing_key=routing_key,
            task_id=task.id,
            sample_id=task.sample_id,
            assigned_channel=subtask.channel if subtask else None,
            execution_policy=policy,
            payload={
                "device": subtask.device if subtask else None,
                "channel": subtask.channel if subtask else None,
                **extra,
            },
        )
        await publish(self._instrument_exchange, routing_key, envelope)

        event_type = routing_key.split(".")[-1]  # e.g. "completed", "failed"
        await self._log_event(
            task_id=task.id,
            sample_id=task.sample_id,
            sample_number=task.sample_number,
            event_type=event_type,
            task_type=task.task_type.value,
            execution_policy=policy,
            device_id=subtask.device if subtask else None,
            channel_index=subtask.channel if subtask else None,
            event_data=extra or None,
        )

    async def _emit_channel_event(
        self, routing_key: str, device: str, channel: int
    ) -> None:
        if self._instrument_exchange is None:
            return
        envelope = build(
            device_id="autocontrol",
            routing_key=routing_key,
            payload={"device": device, "channel": channel},
        )
        await publish(self._instrument_exchange, routing_key, envelope)

    async def _emit_lightweight(self, routing_key: str) -> None:
        if self._instrument_exchange is None:
            return
        envelope = build(device_id="autocontrol", routing_key=routing_key)
        await publish(self._instrument_exchange, routing_key, envelope)

    async def _publish_device_command(self, task: Task, subtask) -> None:
        if self._instrument_exchange is None:
            return
        policy = _EXECUTION_POLICY.get(task.task_type, "infrastructure")
        rk = command_key(subtask.device, CMD_SUBMIT_TASK)
        envelope = build(
            device_id="autocontrol",
            routing_key=rk,
            task_id=task.id,
            sample_id=task.sample_id,
            assigned_channel=subtask.channel,
            execution_policy=policy,
            payload={
                "task_type": task.task_type.value,
                "method_data": subtask.method_data or {},
                "device": subtask.device,
                "channel": subtask.channel,
                "subtask_id": str(subtask.id),
                "acquisition_time": subtask.acquisition_time,
            },
        )
        await publish(self._instrument_exchange, rk, envelope)

    # ------------------------------------------------------------------
    # Audit log
    # ------------------------------------------------------------------

    async def _log_event(
        self,
        *,
        task_id,
        sample_id,
        sample_number,
        event_type: str,
        task_type: str = None,
        execution_policy: str = None,
        device_id: str = None,
        channel_index: int = None,
        event_data: dict = None,
    ) -> None:
        if self._pg_pool is None:
            return
        import json as _json
        try:
            async with self._pg_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO task_events
                        (task_id, sample_id, sample_number, event_type, task_type,
                         execution_policy, device_id, channel_index, event_data)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                    task_id,
                    sample_id,
                    sample_number,
                    event_type,
                    task_type,
                    execution_policy,
                    device_id,
                    channel_index,
                    _json.dumps(event_data) if event_data else None,
                )
        except Exception:
            logger.exception("Failed to write audit log row for task %s", task_id)

    # ------------------------------------------------------------------
    # Inbound: command.autocontrol.# handler
    # ------------------------------------------------------------------

    async def _on_command(
        self, envelope: Envelope, message: aio_pika.abc.AbstractIncomingMessage
    ) -> None:
        rk = message.routing_key or ""
        verb = rk.split(".")[-1]
        payload = envelope.payload

        if verb == "submit_task":
            try:
                task = Task(**payload)
            except (ValidationError, TypeError) as exc:
                logger.error("submit_task: invalid payload: %s", exc)
                raise
            success, task_id, sample_number, response = self.atc.queue_put(task=task)
            if success:
                self.wakeup.set()
                await self._emit_lightweight(SCHEDULER_QUEUE_UPDATED)
                await self._log_event(
                    task_id=task.id,
                    sample_id=task.sample_id,
                    sample_number=sample_number,
                    event_type="queued",
                    task_type=task.task_type.value,
                    execution_policy=_EXECUTION_POLICY.get(task.task_type, "infrastructure"),
                )
            else:
                logger.warning("submit_task rejected by scheduler: %s", response)

        elif verb == "cancel_task":
            task_id = payload.get("task_id")
            if not task_id:
                raise ValueError("cancel_task missing task_id")
            cancelled = self.atc.queue_cancel(task_id=task_id)
            if cancelled:
                with self._pending_subtasks_lock:
                    self._pending_subtasks.pop(str(cancelled.id), None)
                await self._emit_lightweight(SCHEDULER_QUEUE_UPDATED)
                await self._log_event(
                    task_id=cancelled.id,
                    sample_id=cancelled.sample_id,
                    sample_number=cancelled.sample_number,
                    event_type="cancelled",
                    task_type=cancelled.task_type.value,
                )

        elif verb == "resubmit_task":
            task_id = payload.get("task_id")
            if not task_id:
                raise ValueError("resubmit_task missing task_id")
            was_paused = self.atc.paused
            if not was_paused:
                self.atc.paused = True
            old_task = self.atc.queue_cancel(
                task_id=task_id, include_active_queue=True, drop_material=False
            )
            if old_task is None:
                self.atc.paused = was_paused
                raise ValueError(f"resubmit_task: task {task_id} not found")
            if "task" in payload:
                try:
                    new_task = Task(**payload["task"])
                    new_task.priority = old_task.priority
                except (ValidationError, TypeError) as exc:
                    self.atc.paused = was_paused
                    logger.error("resubmit_task: invalid task payload: %s", exc)
                    raise
            else:
                new_task = old_task
            self.atc.queue_put(task=new_task)
            self.atc.paused = was_paused
            await self._emit_lightweight(SCHEDULER_QUEUE_UPDATED)
            await self._log_event(
                task_id=new_task.id,
                sample_id=new_task.sample_id,
                sample_number=new_task.sample_number,
                event_type="retried",
                task_type=new_task.task_type.value,
            )

        elif verb == "pause":
            self.atc.paused = True

        elif verb == "resume":
            self.atc.paused = False

        elif verb == "reset":
            self.atc.reset()
            await self._emit_lightweight(SCHEDULER_QUEUE_UPDATED)

        else:
            logger.warning("Unknown autocontrol command verb '%s' on key '%s'", verb, rk)

    # ------------------------------------------------------------------
    # Inbound: task.completed / task.failed from devices
    # ------------------------------------------------------------------

    async def _on_device_event(
        self, envelope: Envelope, message: aio_pika.abc.AbstractIncomingMessage
    ) -> None:
        rk = message.routing_key or ""
        task_id_str = str(envelope.task_id)

        if TASK_COMPLETED in rk:
            with self._pending_subtasks_lock:
                remaining = self._pending_subtasks.get(task_id_str)
                if remaining is not None:
                    remaining -= 1
                    if remaining > 0:
                        self._pending_subtasks[task_id_str] = remaining
                        return  # wait for remaining subtasks to complete
                    del self._pending_subtasks[task_id_str]
            self.completion_events.put((task_id_str, "completed", envelope))
        else:
            # Failure: forward immediately and stop waiting for this task.
            with self._pending_subtasks_lock:
                self._pending_subtasks.pop(task_id_str, None)
            self.completion_events.put((task_id_str, "failed", envelope))

        self.wakeup.set()

    # ------------------------------------------------------------------
    # Inbound: device.registered from device services
    # ------------------------------------------------------------------

    async def _on_device_registered(
        self, envelope: Envelope, message: aio_pika.abc.AbstractIncomingMessage
    ) -> None:
        payload = envelope.payload
        device_name = payload.get("device_id", "")
        device_type = payload.get("device_type", "")
        device_address = payload.get("address", "")
        num_channels = int(payload.get("num_channels", 1))
        sample_mixing = bool(payload.get("allow_sample_mixing", True))

        def _register() -> None:
            from autocontrol.devices.device_injection import injection_device, distribution_device
            from autocontrol.devices.device_liquid_handler import lh_device
            from autocontrol.devices.device_qcmd import open_QCMD
            from autocontrol.devices.device_rinse import rinse_device

            dt = device_type.lower()
            if dt == 'injection':
                dev = injection_device(name=device_name, address=device_address)
            elif dt == 'lh':
                dev = lh_device(name=device_name, address=device_address)
            elif dt == 'qcmd':
                dev = open_QCMD(name=device_name, address=device_address)
            elif dt == 'rinse':
                dev = rinse_device(name=device_name, address=device_address)
            elif dt == 'distribution':
                dev = distribution_device(name=device_name, address=device_address)
            else:
                logger.warning("device.registered: device '%s' has unknown device_type '%s'", device_name, device_type)
                return

            dev.number_of_channels = num_channels
            if self.atc.device_created_hook is not None:
                self.atc.device_created_hook(dev)

            self.atc.devices.setdefault(device_name, {})
            self.atc.devices[device_name]['device_object'] = dev
            self.atc.devices[device_name]['device_type'] = device_type
            self.atc.devices[device_name]['device_address'] = device_address
            self.atc.devices[device_name]['sample_mixing'] = sample_mixing

            # Pre-populate channel_po only if not already present (idempotent).
            if device_name not in self.atc.channel_po:
                self.atc.channel_po[device_name] = [None] * num_channels
                self.atc.store_channel_po()

            logger.info(
                "device.registered: pre-registered '%s' (%s, %d ch)",
                device_name, device_type, num_channels,
            )

        await asyncio.to_thread(_register)
        self.wakeup.set()

    # ------------------------------------------------------------------
    # Async main loop
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        # Optional PostgreSQL audit log — skip gracefully if unavailable
        pg_dsn = os.getenv("POSTGRES_DSN", "postgresql://roadbot:roadbot_dev@localhost/roadbot")
        try:
            import asyncpg
            self._pg_pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=3)
            logger.info("Connected to PostgreSQL audit log.")
        except Exception:
            logger.warning(
                "PostgreSQL audit log unavailable — task_events will not be written.",
                exc_info=True,
            )

        connection = await get_connection()
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=1)
            await declare_topology(channel)

            self._instrument_exchange = await channel.get_exchange(INSTRUMENT_EXCHANGE)

            cmd_queue = await declare_node_queue(
                channel, "autocontrol", INSTRUMENT_EXCHANGE
            )
            completed_queue = await declare_event_queue(
                channel,
                queue_name="autocontrol.device_completed",
                exchange_name=INSTRUMENT_EXCHANGE,
                routing_key_pattern=TASK_COMPLETED,
            )
            failed_queue = await declare_event_queue(
                channel,
                queue_name="autocontrol.device_failed",
                exchange_name=INSTRUMENT_EXCHANGE,
                routing_key_pattern=TASK_FAILED,
            )

            # Transient queue for device registration — auto-deletes on disconnect
            # so stale announcements never pile up across restarts.
            reg_queue = await channel.declare_queue(
                "autocontrol.device_registrations",
                durable=False,
                auto_delete=True,
            )
            await reg_queue.bind(self._instrument_exchange, routing_key=DEVICE_REGISTERED)

            logger.info("Broker worker running.")
            await asyncio.gather(
                consume(cmd_queue, self._on_command),
                consume(completed_queue, self._on_device_event),
                consume(failed_queue, self._on_device_event),
                consume(reg_queue, self._on_device_registered),
            )

    # ------------------------------------------------------------------
    # Start
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the broker worker in a background daemon thread."""

        def _thread_main() -> None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            try:
                self._loop.run_until_complete(self._run())
            except Exception:
                logger.exception("Broker worker exited with error.")
            finally:
                self._loop.close()

        self._thread = threading.Thread(
            target=_thread_main, name="broker-worker", daemon=True
        )
        self._thread.start()
        logger.info("Broker worker thread started.")
