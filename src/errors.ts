/**
 * errors.ts — control-flow signals and error (de)serialization.
 *
 * WorkflowSuspended is NOT a failure. It is a control-flow signal thrown to
 * unwind the workflow function cleanly when it hits a durable wait (a pending
 * timer). The runtime catches it and leaves the workflow in 'sleeping' state
 * instead of marking it failed. Later, when the timer fires, we replay from the
 * top and this time the sleep sees TIMER_FIRED and returns instead of throwing.
 */
export class WorkflowSuspended extends Error {
  readonly kind = "WorkflowSuspended" as const;
  constructor(
    public readonly reason: string,
    public readonly seq: number,
    public readonly wakeAt: number,
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
