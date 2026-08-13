/**
 * errors.ts — control-flow signals and error (de)serialization.
 *
 * WorkflowSuspended is NOT a failure. It is a control-flow signal thrown to
 * unwind the workflow function cleanly when it hits a durable wait. The runtime
 * catches it and parks the workflow (instead of marking it failed) so it can be
 * resumed later by replaying from the top. There are two flavors:
 *
 *   - reason "timer":  waiting on a durable timer. Parked as 'sleeping' with a
 *     wakeAt; the scheduler appends TIMER_FIRED when due and replays. On replay
 *     the sleep() sees TIMER_FIRED and returns instead of throwing.
 *   - reason "signal": waiting on an external signal. Parked as 'waiting' (no
 *     wakeAt — a wait has no scheduled time). Delivery of the signal flips the
 *     run back to 'running' and replays; the wait then finds the buffered
 *     SIGNAL_RECEIVED, binds it, and returns instead of throwing.
 */
export class WorkflowSuspended extends Error {
  readonly kind = "WorkflowSuspended" as const;
  constructor(
    public readonly reason: "timer" | "signal",
    public readonly seq: number,
    /** Epoch ms to wake a timer wait; unused (0) for a signal wait. */
    public readonly wakeAt: number = 0,
  ) {
    super(`WorkflowSuspended(seq=${seq}, reason=${reason}, wakeAt=${wakeAt})`);
    this.name = "WorkflowSuspended";
  }
}

export function isWorkflowSuspended(err: unknown): err is WorkflowSuspended {
  return err instanceof WorkflowSuspended ||
    (typeof err === "object" && err !== null && (err as any).kind === "WorkflowSuspended");
}

/**
 * Thrown when replay drifts: a ctx.* call lands on a seq that a previous run
 * recorded as a DIFFERENT kind of step (e.g. an activity() where a sleep() used
 * to be). That only happens if the workflow body is non-deterministic — the one
 * thing the whole engine forbids. Failing loudly beats silently returning the
 * wrong logged result.
 */
export class DeterminismError extends Error {
  readonly kind = "DeterminismError" as const;
  constructor(message: string) {
    super(message);
    this.name = "DeterminismError";
  }
}

/** Shape we persist for a permanently-failed activity so replay can re-throw it. */
export interface SerializedError {
  name: string;
  message: string;
  stack?: string;
}

export function serializeError(err: unknown): SerializedError {
  if (err instanceof Error) {
    return { name: err.name, message: err.message, stack: err.stack };
  }
  // Non-Error throwables: stringify best-effort.
  return { name: "Error", message: typeof err === "string" ? err : JSON.stringify(err) };
}

/**
 * Rebuild an Error from a serialized payload. Used on replay so a logged
 * ACTIVITY_FAILED re-throws deterministically WITHOUT re-running the side effect.
 */
export function rebuildError(payload: unknown): Error {
  const p = (payload ?? {}) as Partial<SerializedError>;
  const e = new Error(p.message ?? "activity failed");
  if (p.name) e.name = p.name;
  // Preserve the original stack for observability; mark it as a replayed error.
  (e as any).replayed = true;
  return e;
}
