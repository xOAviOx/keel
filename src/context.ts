import type { EventLog } from "./eventLog";
import { DeterminismError, rebuildError, serializeError, WorkflowSuspended } from "./errors";

/**
 * context.ts — WorkflowContext. THE HEART OF THE ENGINE.
 *
 * ============================ THE ONE BIG IDEA ============================
 * A workflow is a DETERMINISTIC function. Every side effect goes through
 * `ctx.activity(...)`, whose result is written to the append-only event log.
 *
 * On restart we RE-RUN the workflow function from the very top. Each ctx call
 * consumes the next `seq` (0,1,2,...). Before running a side effect, we check
 * the log for a completed result at that seq:
 *   - If found  -> return the logged value; DO NOT run the side effect again.
 *   - If absent -> this is the first real execution; run it and log the result.
 *
 * Because the workflow body is deterministic, the Nth ctx call ALWAYS gets the
 * same seq on every run, so logged results line up perfectly with new calls.
 * That is the entire trick: deterministic replay + memoized side effects.
 * =========================================================================
 *
 * ========================== DETERMINISM RULES ============================
 * Inside a workflow function you MUST NOT use any of:
 *   - Date.now() / new Date()      -> use ctx.now()
 *   - Math.random()                -> use ctx.random()
 *   - setTimeout / timers          -> use ctx.sleep(ms)
 *   - direct file / network / db I/O, console side effects that matter, or
 *     non-deterministic iteration order (e.g. Object key order from a Map that
 *     was built non-deterministically).
 * ALL real-world effects go through ctx.activity(). The workflow body itself is
 * PURE ORCHESTRATION. Break these rules and replay will drift: a ctx call will
 * land on the wrong seq and read someone else's logged result. (now/random/
 * sleep are added in Phase 5.)
 * =========================================================================
 */
/** Per-activity reliability options. */
export interface ActivityOptions {
  /** Number of RETRIES after the first attempt (total attempts = retries + 1). */
  retries?: number;
  /** Base backoff in ms; the delay before retry N is backoffMs * 2^N. */
  backoffMs?: number;
}

export class WorkflowContext {
  /** Deterministic call index. Advances by exactly 1 per ctx.* call. */
  private seq = 0;

  constructor(
    public readonly workflowId: string,
    private readonly log: EventLog,
  ) {}

  /** For tests/observability: how many ctx calls have happened this run. */
  get calls(): number {
    return this.seq;
  }

  /**
   * Run a side effect exactly once, ever, across all runs of this workflow.
   *
   * @param name human-readable activity name (recorded for observability)
   * @param fn   the side effect. Called ONLY on first execution. Never on replay.
   */
  async activity<T>(
    name: string,
    fn: () => Promise<T> | T,
    options: ActivityOptions = {},
  ): Promise<T> {
    const currentSeq = this.seq++; // deterministic index for THIS call

    // Guardrail: if a TIMER lived at this seq on a prior run, the body drifted.
    this.assertNoTimerAt(currentSeq, "activity");

    // --- REPLAY PATHS: a prior run already resolved this activity. ---
    const done = this.log.findEvent(this.workflowId, currentSeq, "ACTIVITY_COMPLETED");
    if (done) {
      // Return the logged result. fn is NOT invoked. This is what makes a
      // completed side effect (a charge, an email) never happen twice.
      return (done.payload as { value: T }).value;
    }

    const failed = this.log.findEvent(this.workflowId, currentSeq, "ACTIVITY_FAILED");
    if (failed) {
      // A prior run decided this activity permanently failed. Re-throw the same
      // error deterministically, again WITHOUT re-running fn.
      throw rebuildError(failed.payload);
    }

    // --- FIRST real execution of this activity. ---
    // Retries happen WITHIN this single execution attempt. The log never sees
    // the intermediate failures — only the FINAL outcome (one ACTIVITY_COMPLETED
    // or one ACTIVITY_FAILED) is written. That's what keeps replay deterministic:
    // regardless of how many times fn was retried, replay sees exactly one result.
    const retries = Math.max(0, options.retries ?? 0);
    const backoffMs = Math.max(0, options.backoffMs ?? 0);

    let lastErr: unknown;
    for (let attempt = 0; attempt <= retries; attempt++) {
      try {
        const value = await fn();
        // Wrap as { value } so null/undefined round-trip through JSON.
        this.log.append(this.workflowId, currentSeq, "ACTIVITY_COMPLETED", { value, name });
        return value;
      } catch (err) {
        lastErr = err;
        if (attempt < retries) {
          // Exponential backoff: backoffMs * 2^attempt. This is a REAL in-worker
          // wait (not a durable ctx.sleep) because it's internal to this attempt.
          await realDelay(backoffMs * 2 ** attempt);
          continue;
        }
      }
    }

    // All attempts exhausted -> permanent failure. Log once; it will re-throw
    // deterministically on any future replay (fn is never re-run). We store the
    // ACTIVITY name under `activity` so it doesn't collide with the Error's own
    // `name` (which serializeError provides for faithful rebuild on replay).
    this.log.append(this.workflowId, currentSeq, "ACTIVITY_FAILED", {
      activity: name,
      ...serializeError(lastErr),
    });
    throw lastErr;
  }

  // ---------------------------------------------------------------------------
  // Durable non-determinism: now() and random()
  //
  // Workflow code must NEVER call Date.now() or Math.random() directly. These
  // capture the value on the first run, log it, and REPLAY the same value
  // forever after. Each consumes its own seq, exactly like an activity.
  // ---------------------------------------------------------------------------

  /** Durable wall-clock time. Same value on every replay. */
  now(): number {
    return this.durableValue("__now", () => Date.now());
  }

  /** Durable random in [0,1). Same value on every replay. */
  random(): number {
    return this.durableValue("__random", () => Math.random());
  }

  /**
   * Shared logic for now()/random(): consume a seq, replay the logged value if
   * present, otherwise generate it once and log it as an ACTIVITY_COMPLETED.
   */
  private durableValue(marker: string, generate: () => number): number {
    const currentSeq = this.seq++;
    this.assertNoTimerAt(currentSeq, marker);

    const done = this.log.findEvent(this.workflowId, currentSeq, "ACTIVITY_COMPLETED");
    if (done) return (done.payload as { value: number }).value;

    const value = generate();
    this.log.append(this.workflowId, currentSeq, "ACTIVITY_COMPLETED", { value, name: marker });
    return value;
  }

  // ---------------------------------------------------------------------------
  // Durable timer: sleep()
  //
  // sleep() does NOT block. On first hit it records a TIMER_STARTED with an
  // absolute fireAt and throws WorkflowSuspended to unwind the run; the runtime
  // parks the workflow as 'sleeping'. The scheduler later appends TIMER_FIRED
  // when the timer is due and replays the workflow — at which point sleep() sees
  // TIMER_FIRED and simply returns, so execution continues past the sleep.
  //
  // Note: fireAt is computed once (first hit) and persisted, then read back on
  // every replay — so we don't need ctx.now() here; the persisted value already
  // makes the wake time deterministic.
  // ---------------------------------------------------------------------------
  async sleep(ms: number): Promise<void> {
    const currentSeq = this.seq++;

    // Guardrail: if an activity/now/random lived at this seq before, body drifted.
    this.assertNoActivityAt(currentSeq, "sleep");

    // The scheduler fired it -> continue past the sleep.
    if (this.log.findEvent(this.workflowId, currentSeq, "TIMER_FIRED")) return;

    // Already scheduled but not yet fired -> suspend again with the SAME fireAt.
    const started = this.log.findEvent(this.workflowId, currentSeq, "TIMER_STARTED");
    if (started) {
      const fireAt = (started.payload as { fireAt: number }).fireAt;
      throw new WorkflowSuspended("timer", currentSeq, fireAt);
    }

    // First hit: schedule the timer, then suspend.
    const fireAt = Date.now() + ms;
    this.log.append(this.workflowId, currentSeq, "TIMER_STARTED", { fireAt, ms });
    throw new WorkflowSuspended("timer", currentSeq, fireAt);
  }

  // ---------------------------------------------------------------------------
  // Determinism guardrails
  // ---------------------------------------------------------------------------

  private assertNoTimerAt(seq: number, who: string): void {
    if (
      this.log.findEvent(this.workflowId, seq, "TIMER_STARTED") ||
      this.log.findEvent(this.workflowId, seq, "TIMER_FIRED")
    ) {
      throw new DeterminismError(
        `Non-determinism at seq=${seq}: ${who}() found a TIMER here on a prior run. ` +
          `The workflow body changed between runs (a sleep() became a ${who}()).`,
      );
    }
  }

  private assertNoActivityAt(seq: number, who: string): void {
    if (
      this.log.findEvent(this.workflowId, seq, "ACTIVITY_COMPLETED") ||
      this.log.findEvent(this.workflowId, seq, "ACTIVITY_FAILED")
    ) {
      throw new DeterminismError(
        `Non-determinism at seq=${seq}: ${who}() found an ACTIVITY here on a prior run. ` +
          `The workflow body changed between runs (an activity/now/random became a ${who}()).`,
      );
    }
  }
}

/** A real wall-clock delay (used only for in-attempt retry backoff). */
function realDelay(ms: number): Promise<void> {
  if (ms <= 0) return Promise.resolve();
  return new Promise((resolve) => setTimeout(resolve, ms));
}
