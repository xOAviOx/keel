# durable-engine — a local "mini-Temporal"

[![CI](https://github.com/xOAviOx/keel/actions/workflows/ci.yml/badge.svg)](https://github.com/xOAviOx/keel/actions/workflows/ci.yml)

A tiny **durable execution engine** you can run on a laptop with nothing but Node.
A running workflow is **crash-proof**: kill the process at any point (even `kill -9`)
and, on restart, the workflow **resumes exactly where it left off** and **never
re-runs a side effect that already completed**.

100% free and local. The entire persistence layer is one embedded SQLite file.
No cloud, no queues, no API keys, no network — ever.

---

## The replay model in 5 sentences

1. A workflow is a **deterministic function**; every side effect runs through
   `ctx.activity(...)`, whose result is written to an **append-only event log**.
2. On restart the engine **re-runs the workflow function from the very top**.
3. Each `ctx.*` call consumes the next **`seq`** (0, 1, 2, …); before running a
   side effect, the engine checks the log for a completed result at that `seq`.
4. If a result exists it is **returned from the log and the side effect is not
   re-executed**; if not, this is the first execution, so it runs and logs the result.
5. Because the body is deterministic, the Nth `ctx` call always lands on the same
   `seq`, so logged results line up perfectly with new calls — that is the whole trick.

---

## Quick start

```bash
npm install
npm test          # runs the full phased test suite
```

### The crash demo (exactly-once charge)

```bash
# 1) Start the order workflow, but hard-crash right after the charge is committed.
CRASH_AFTER=charge npm run start orderFlow '{"orderId":"A1"}'
#   -> "[charge] charged A1 (charge count is now 1)"
#   -> "💥 CRASH_AFTER=charge: killing process (exit 1)"   (process dies)

# 2) Recover. The run resumes at sendEmail and does NOT charge again.
npm run cli recover
#   -> "[sendEmail] confirmation email sent for A1"
#   -> "[followUp] follow-up sent for A1"

# 3) Inspect the durable history.
npm run cli history <workflow_id>     # id was printed in step 1
cat data/charges.txt                  # -> 1   (charged exactly once)
```

Crash points you can try: `CRASH_AFTER=charge`, `CRASH_AFTER=sendEmail`,
`CRASH_AFTER=followUp`. In every case the charge counter ends at exactly `1`.

### The sleep demo (durable timer across a restart)

```bash
# Parks itself as 'sleeping' and exits — the process is gone during the sleep.
npm run start sleepFlow '{"sleepMs":5000,"label":"demo"}'
npm run cli list                      # -> status: sleeping, wake_at=...

# The worker recovers + runs a scheduler loop; it fires the timer when due.
npm run worker                        # Ctrl-C to stop
npm run cli list                      # -> status: completed
```

### CLI

```
npm run cli start <name> [jsonInput]   kick off a workflow (runs to first stop)
npm run cli worker                     recovery + scheduler loop (long-running)
npm run cli recover                    one-shot: re-run interrupted ('running') runs
npm run cli history <workflow_id>      ordered event log
npm run cli status <workflow_id>       status + wake_at
npm run cli list                       all workflows
```

The DB path defaults to `./data/engine.db`; override with `ENGINE_DB=/path/to.db`.
The demo charge counter defaults to `./data/charges.txt`; override with `CHARGE_FILE`.

---

## Determinism rules (read before writing a workflow)

The workflow body is **pure orchestration**. Inside a workflow function you must
**never** use any of the following, because they break replay alignment:

- `Date.now()` / `new Date()` → use **`ctx.now()`** (durable, replay-stable)
- `Math.random()` → use **`ctx.random()`** (durable, replay-stable)
- `setTimeout` / timers → use **`ctx.sleep(ms)`** (durable timer)
- direct file / network / DB I/O, or any other real-world effect → wrap it in
  **`ctx.activity(name, fn, opts?)`**
- non-deterministic iteration order (e.g. iterating a Set/Map built from
  unordered input)

Every real effect goes through `ctx.activity`. If the body drifts between runs
(a `sleep()` becomes an `activity()`, say), the engine throws a
**`DeterminismError`** instead of silently returning the wrong logged result.

### Reliability

`ctx.activity(name, fn, { retries, backoffMs })` retries `fn` up to `retries`
times with exponential backoff (`backoffMs * 2^attempt`) **within a single
execution**. Only the final outcome is logged, so replay stays deterministic. If
all attempts fail, an `ACTIVITY_FAILED` is written, the workflow is marked
`failed`, and that failure re-throws deterministically on any future replay.

### The one unavoidable window

A crash strictly *between* an external side effect and the log append is the
inherent at-least-once window every such system has. The demo crashes at the safe
point (after the completion is committed); real activities are made **idempotent**
to cover the window. `synchronous = NORMAL` in WAL mode is durable across process
crashes (our whole point); full power-loss durability would use `FULL`.

---

## Architecture

```
                       ┌─────────────────────────────────────────┐
   npm run start  ─────►                CLI (cli.ts)              │
   npm run worker ─────►   start · recover · history · status     │
                       └───────────────┬─────────────────────────┘
                                       │
                       ┌───────────────▼─────────────────────────┐
                       │              Runtime (runtime.ts)        │
                       │  createRun · runWorkflow · recover()     │
                       │  startWorkflow · fireDueTimer            │
                       └───┬───────────────────┬─────────────┬────┘
                           │                   │             │
             replays with  │        parks/wakes│      looks  │ up fn by name
                           ▼                   ▼             ▼
              ┌────────────────────┐  ┌────────────────┐  ┌──────────────┐
              │ WorkflowContext    │  │  Scheduler     │  │  Registry    │
              │ (context.ts)       │  │ (scheduler.ts) │  │ (registry.ts)│
              │  seq counter       │  │  poll due      │  │ name -> fn   │
              │  activity/now/     │  │  timers, fire  │  └──────────────┘
              │  random/sleep      │  │  TIMER_FIRED   │
              └─────────┬──────────┘  └───────┬────────┘
                        │  append / findEvent │  append TIMER_FIRED
                        ▼                     ▼
              ┌─────────────────────────────────────────────┐
              │              EventLog (eventLog.ts)          │
              │        append-only · ordered by id           │
              └───────────────────────┬─────────────────────┘
                                      │
                       ┌──────────────▼──────────────┐
                       │      SQLite (db.ts, WAL)     │
                       │   events + workflow_state    │  ← one local .db file
                       └─────────────────────────────┘
```

### Data model

`events` is the **append-only source of truth** (never updated/deleted):

| column | meaning |
| --- | --- |
| `id` | global append order (autoincrement) |
| `workflow_id` | which run |
| `seq` | deterministic call index within the run (0,1,2…; `-1` for lifecycle markers) |
| `type` | `WORKFLOW_STARTED` · `ACTIVITY_COMPLETED` · `ACTIVITY_FAILED` · `TIMER_STARTED` · `TIMER_FIRED` · `WORKFLOW_COMPLETED` · `WORKFLOW_FAILED` |
| `payload` | JSON (activity results wrapped as `{ value }` so `null`/`undefined` round-trip) |
| `created_at` | epoch ms (observability only) |

`workflow_state` is a small derived index so the worker can quickly find runs to
resume (`status='running'`) and timers that are due (`status='sleeping' AND wake_at<=now`).

Writes that must be atomic together — appending an event **and** flipping
`workflow_state` — are wrapped in a single `better-sqlite3` transaction, so a
crash can never leave a half-applied step.

---

## Project layout

```
durable-engine/
  src/
    db.ts             opens SQLite, WAL, schema
    eventLog.ts       append + query events (source of truth)
    context.ts        WorkflowContext: activity / now / random / sleep  (the heart)
    errors.ts         WorkflowSuspended, DeterminismError, error (de)serialization
    runtime.ts        run + replay workflows, recovery on boot, timer firing
    scheduler.ts      wakes sleeping workflows when timers are due
    registry.ts       workflow name -> function
    workflows/
      orderFlow.ts    charge -> sendEmail -> followUp  (crash demo)
      sleepFlow.ts    A -> sleep -> B                  (durable-timer demo)
    cli.ts            start / recover / history / status / list / worker
    index.ts          wiring + long-running worker entrypoint
  test/               one test file per phase
```

## How recovery works, concretely

- On boot the worker calls `recover()`, which re-runs every workflow left in
  `status='running'` (i.e. mid-execution when the process died). Replay skips
  completed activities and resumes from the first unfinished step.
- Sleeping workflows are **not** recovered directly — they belong to the
  scheduler, which appends `TIMER_FIRED` when `wake_at` passes and replays them.
  `TIMER_FIRED` is written idempotently, so a timer is never double-processed.
- Runs are isolated by `workflow_id`, so any number of them recover independently.
```
