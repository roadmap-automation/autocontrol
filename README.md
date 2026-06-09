# autocontrol — Central Hardware Scheduler

autocontrol is the single authority over all hardware lock decisions in the ROADMAP system.
It maintains a priority queue of `Task` objects, enforces channel affinity across all
instruments, manages per-device/per-channel occupancy locks, and dispatches tasks to
physical device controllers via RabbitMQ.

No instrument has scheduling authority of its own. Devices execute what autocontrol tells
them; they do not negotiate, self-schedule, or decide when to accept the next task.

---

## Role in the System

autocontrol sits at the top of **Tier 2 (Instrument Domain)** on `exchange.instrument`.
It receives tasks from lh_manager, dispatches them to physical devices, and publishes
scheduler events so lh_manager can track progress without polling.

```
lh_manager  ──command.autocontrol.submit_task──►  autocontrol
                                                      │
                      command.<device_id>.submit_task ▼
                                                   devices
                                                      │
                           task.completed / task.failed ▼
autocontrol  ◄──────────────────────────────────── devices
     │
     └──scheduler.task_completed──► lh_manager
```

---

## Scheduling Model

**Priority:** Tasks are prioritised by `sample_number × -1 − time_fraction`. Lower sample
number = higher priority. Ties are broken by submission time.

**Channel affinity (sticky routing):** `channel_mode='reuse'` in `TaskData` forces
`find_free_channels()` to assign a task only to the channel already occupied by the same
sample, tracked in `channel_po[device][channel]`. This enforces that a sample never
switches channels mid-workflow across any instrument.

**Sample mixing prevention:** For devices initialised with `sample_mixing=False`,
autocontrol checks whether the target channel was previously used by a different sample
before dispatching. If so, the task blocks until the channel is clear.

**Dependency chains:** `Task.dependency_id` / `Task.dependency_sample_number` allow a task
to block until a prior task for the same sample completes. Used to enforce ordering within
a workflow (e.g., formulation must complete before injection).

---

## Failure Domains

autocontrol reads the `execution_policy` field from each task envelope to determine how to
respond to a device failure, without needing to know the task type:

| execution_policy | Response |
|---|---|
| `repeatable` | Auto-retry up to the retry limit |
| `uncertain` | Freeze sample; add to `suspended_samples`; alert operator; require manual Clear Fault |
| `irreversible` | Abort sample immediately; publish abort event; release all downstream locks |
| `infrastructure` | Log and alert; no sample abort required |

---

## Broker Interface

### Subscribes to (inbound commands)
| Routing key | Queue | Publisher | Action |
|---|---|---|---|
| `command.autocontrol.submit_task` | `commands.autocontrol` (durable) | lh_manager | Enqueue a new Task |
| `command.autocontrol.cancel_task` | `commands.autocontrol` | lh_manager | Remove a pending task |
| `command.autocontrol.resubmit_task` | `commands.autocontrol` | lh_manager | Requeue a failed task |
| `command.autocontrol.pause` | `commands.autocontrol` | operator UI | Pause the dispatch loop |
| `command.autocontrol.resume` | `commands.autocontrol` | operator UI | Resume the dispatch loop |

### Subscribes to (device completion events)
| Routing key | Publisher | Action |
|---|---|---|
| `task.completed` | any physical device | Release channel lock; publish `scheduler.task_completed` |
| `task.failed` | any physical device | Apply `execution_policy` failure response |
| `device.registered` | any physical device | Construct Device object; set `broker_mode=True`; pre-populate `channel_po` |

### Publishes (outbound commands to devices)
| Routing key | Subscriber | When |
|---|---|---|
| `command.<device_id>.submit_task` | named physical device | When channel is free and task is at head of queue |
| `command.<device_id>.cancel_task` | named physical device | On task cancellation |

### Publishes (scheduler events)
| Routing key | When |
|---|---|
| `scheduler.task_dispatched` | After sending task to device |
| `scheduler.task_completed` | After receiving `task.completed` from device |
| `scheduler.task_failed` | After receiving `task.failed` from device |
| `scheduler.channel_locked` | When a channel is assigned to a task |
| `scheduler.channel_released` | When a channel is freed after completion |
| `scheduler.queue_updated` | When a task is added, cancelled, or resubmitted |

---

## REST Endpoints (remain after broker refactor)

These endpoints are not replaced by the broker. They provide read-only state visibility
for lh_manager's GUI and debugging.

| Endpoint | Purpose |
|---|---|
| `GET /` | Scheduler health and state summary |
| `GET /get_task_status/<task_id>` | Status of a specific task |
| `GET /get_subtask_results/<task_id>/<subtask_id>` | Per-device subtask result |
| `GET /queue_inspect` | Current queue state (JSON) |

---

## Getting Started

```bash
# From the repo root using process-compose (recommended):
process-compose up

# Or directly:
python -m autocontrol.launch
```

autocontrol requires RabbitMQ to be running (`docker compose up -d`). It will
reconnect automatically if the broker restarts. Devices that publish `device.registered`
before autocontrol starts will have their registrations processed when autocontrol
connects and drains its durable command queue.

### Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `AMQP_URL` | `amqp://roadbot:roadbot_dev@localhost/` | RabbitMQ connection |
| `ROADMAP_DB_DSN` | `postgresql://roadbot:roadbot_dev@localhost/roadbot` | PostgreSQL audit log (optional; skips gracefully if unavailable) |
| `AUTOCONTROL_STORAGE_PATH` | — | Path below the DataLad experiment tree for SQLite state files |
