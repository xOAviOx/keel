import type { Runtime } from "./runtime";

/**
 * scheduler.ts — wakes sleeping workflows when their timers come due.
 *
 * A simple poll loop: every `intervalMs`, find sleeping workflows whose wake_at
 * has passed, append TIMER_FIRED for the pending timer, flip them back to
 * 'running', and replay them. On replay the sleep() sees TIMER_FIRED and returns,
 * so execution continues past the sleep (possibly to the next sleep, which
 * re-parks the workflow — each sleep is its own seq/timer).
 */
export class Scheduler {
  private handle: NodeJS.Timeout | undefined;
  private running = false;
  private readonly intervalMs: number;

  constructor(
    private readonly runtime: Runtime,
    opts: { intervalMs?: number } = {},
  ) {
    this.intervalMs =
      opts.intervalMs ??
      (process.env.SCHED_INTERVAL_MS ? Number(process.env.SCHED_INTERVAL_MS) : 1000);
  }

  /**
   * Process all currently-due timers exactly once. Returns the ids of the
   * workflows advanced this tick. Safe to call directly (tests do).
   */
  async tick(now: number = Date.now()): Promise<string[]> {
    const due = this.runtime.dueSleepingWorkflows(now);
    const advanced: string[] = [];
    for (const id of due) {
      // fireDueTimer is idempotent + transactional; guards double-processing.
      if (this.runtime.fireDueTimer(id, now)) {
        await this.runtime.runWorkflow(id); // replay: sleep returns, run continues
        advanced.push(id);
      }
    }
    return advanced;
  }

  /** Start the background poll loop. Non-overlapping ticks. */
  start(): void {
    if (this.handle) return;
    const loop = async () => {
      if (this.running) return; // don't overlap a slow tick
      this.running = true;
      try {
        const advanced = await this.tick();
        if (advanced.length > 0) {
          console.log(`[scheduler] advanced ${advanced.length} workflow(s): ${advanced.join(", ")}`);
        }
      } catch (err) {
        console.error("[scheduler] tick error:", err);
      } finally {
        this.running = false;
      }
    };
    this.handle = setInterval(loop, this.intervalMs);
    // Don't keep the event loop alive purely for the scheduler in tests.
    if (typeof this.handle.unref === "function") this.handle.unref();
  }

  stop(): void {
    if (this.handle) {
      clearInterval(this.handle);
      this.handle = undefined;
    }
  }
}
