# autocontrol — Refactor Context for AI Assistant

## 1. Core Responsibility
`autocontrol` is the central hardware scheduler: it maintains a priority queue of `Task` objects, enforces channel affinity (sticky routing) across all devices, manages per-device/per-channel physical occupancy locks, and polls device HTTP endpoints to detect task completion before releasing locks and dispatching the next task.

---

## 2. Physical Constraints

- **Central lock enforcer — instruments cannot self-schedule.** `autocontrol` is the single authority over which task runs on which physical channel. All hardware locking decisions (channel allocation, sample mixing prevention, simultaneous multi-device locks) are made here, not by the devices.
- **Channel affinity (sticky routing).** `channel_mode='reuse'` in `TaskData` forces `find_free_channels()` to assign a task only to the channel already occupied by the same sample (tracked via `channel_po` dict: `device → [task|None per channel]`). This is the mechanism that enforces sticky channel assignment across all downstream instruments.
- **Sample mixing prevention.** For devices initialized with `sample_mixing=False`, `atc.py` performs a route-check before dispatch: if a target channel was previously used by a different sample's task, the task is blocked until the channel is clear, preventing cross-contamination.
- **Priority = sample recency.** Task priority is computed as `sample_number × -1 − time_fraction`. Lower sample_number = higher priority. Ties broken by submission time.
- **Three task error domains (enforced here for routing, not execution).** `TaskType.PREPARE` → Irreversible; `TaskType.TRANSFER` → Uncertain; `TaskType.MEASURE` → Repeatable. The refactored broker consumer must preserve these routing rules when determining retry/abort behavior after a device publishes a failure event.
- **Dependency chains.** `Task.dependency_id` / `Task.dependency_sample_number` allow a task to block until a prior task for the same sample completes. This ordering must be preserved in the broker model.
- **Polling-based completion detection.** `Device.get_status()` GETs `/GetStatus` on each registered device every 5 seconds (0.1 s burst after recent activity). The broker refactor replaces this poll loop with device-published `Task.Completed` / `Task.Failed` events; the active-task loop becomes a subscription.

---

## 3. Deprecated REST Endpoints

### Inbound (from lh_manager / operator)
- `POST /put` — submit a `Task` to the priority queue
- `POST /cancel` — cancel a pending or active task
- `POST /resubmit` — resubmit a previously dispatched task
- `POST /pause` / `POST /resume` — pause/resume the dispatch loop
- `POST /reset` / `POST /restart` — reset or restart the scheduler
- `POST /shutdown` — shutdown with optional queue drain

### Outbound (autocontrol → devices — polled)
- `POST /SubmitTask` on each device — delivers `TaskData` to physical device controller
- `GET /GetStatus` on each device — polls IDLE/BUSY/channel status every 5 s

### Query (called by lh_manager for status display — likely stay REST)
- `GET /get_task_status/<task_id>` — status of a specific task
- `GET /get_subtask_results/<task_id>/<subtask_id>` — result of a subtask
- `GET /queue_inspect` — current queue state
- `GET /` — scheduler health/state summary

---

## 4. Future Telemetry Needs

### State change events (publish on change)
- `Scheduler.TaskDispatched` — task dequeued and sent to device; payload: `{task_id, device, channel, sample_id}`
- `Scheduler.TaskCompleted` — device reported success, channel released; payload: `{task_id, device, channel}`
- `Scheduler.TaskFailed` — device reported failure; payload: `{task_id, device, channel, error_domain: repeatable|uncertain|irreversible}`
- `Scheduler.ChannelLocked` / `Scheduler.ChannelReleased` — per-device channel occupancy transitions (replaces `channel_po` polling by lh_manager)
- `Scheduler.QueueUpdated` — task added, cancelled, or resubmitted (lightweight; no queue dump in message body)

### Inbound broker subscriptions (replace inbound HTTP)
- Subscribe to `Task.Completed` / `Task.Failed` from device controllers → replaces polling `GET /GetStatus`
- Subscribe to `Command.SubmitTask` / `Command.CancelTask` from lh_manager → replaces `POST /put` / `POST /cancel`

### Large data (Claim Check — do not carry in broker message body)
- Queue snapshots for GUI debug: save to SQLite (`active_queue`, `history_queue`); expose via `GET /queue_inspect` (stays REST)
- Subtask results: fetched via `GET /get_subtask_results/...` after receiving a `Scheduler.TaskCompleted` event (stays REST)
