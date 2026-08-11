import type { WorkflowContext } from "../context";

/**
 * sleepFlow.ts — demo workflow that exercises a durable timer:
 *   activityA -> sleep(ms) -> activityB
 *
 * Used to prove that a workflow can be sleeping across a full process restart
 * and still fire (via the Scheduler) once the timer is due.
 */
export interface SleepInput {
  sleepMs?: number;
  label?: string;
}

export async function sleepFlow(ctx: WorkflowContext, input: SleepInput): Promise<string> {
  const label = input?.label ?? "sleep-demo";
  const ms = input?.sleepMs ?? 200;

  await ctx.activity("A", () => {
    console.log(`  [A] before sleep (${label})`);
    return "A-done";
  });

  await ctx.sleep(ms); // suspends here; scheduler resumes when due

  await ctx.activity("B", () => {
    console.log(`  [B] after sleep (${label})`);
    return "B-done";
  });

  return `${label}: A -> slept ${ms}ms -> B`;
}
