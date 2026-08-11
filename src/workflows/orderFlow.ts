import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import type { WorkflowContext } from "../context";

/**
 * orderFlow.ts — the demo workflow used to PROVE exactly-once side effects
 * across a real process crash.
 *
 * Steps (each is a ctx.activity, so each is memoized in the event log):
 *   charge()    -> increments an ON-DISK charge counter (stands in for a real
 *                  payment provider) and logs "charged".
 *   sendEmail() -> logs "email sent".
 *   followUp()  -> logs "follow-up sent".
 *
 * Crash injection: set CRASH_AFTER=charge|sendEmail|followUp to hard-kill the
 * process with process.exit(1) IMMEDIATELY AFTER that step's activity has
 * returned — which means its ACTIVITY_COMPLETED event is already durably
 * committed to the WAL. On the next boot, recovery replays that step from the
 * log and DOES NOT run the side effect again. That's the whole proof.
 *
 * Why the crash is placed AFTER `await ctx.activity(...)` and not inside the
 * side effect: the only moment it is safe to lose the process is once the
 * completion is on disk. Crashing between the external effect and the log write
 * is the inherent at-least-once window every such system has; real activities
 * are made idempotent to cover it. Our demo crashes at the safe point.
 */

/** External-world charge counter file (separate from the engine DB on purpose). */
function chargeFilePath(): string {
  return process.env.CHARGE_FILE ?? resolve(process.cwd(), "data", "charges.txt");
}

export function readChargeCount(): number {
  try {
    const raw = readFileSync(chargeFilePath(), "utf8").trim();
    const n = Number.parseInt(raw, 10);
    return Number.isFinite(n) ? n : 0;
  } catch {
    return 0; // file not created yet -> zero charges
  }
}

function incrementCharge(): number {
  const path = chargeFilePath();
  mkdirSync(dirname(path), { recursive: true });
  const next = readChargeCount() + 1;
  writeFileSync(path, String(next), "utf8");
  return next;
}

export function resetChargeCount(): void {
  writeFileSync(chargeFilePath(), "0", "utf8");
}

/** Hard crash hook. No-op unless CRASH_AFTER matches this step. */
function maybeCrash(step: string): void {
  if (process.env.CRASH_AFTER === step) {
    // The step's completion event is already committed to disk at this point.
    console.error(`💥 CRASH_AFTER=${step}: killing process (exit 1)`);
    process.exit(1);
  }
}

export interface OrderInput {
  orderId?: string;
}

export async function orderFlow(ctx: WorkflowContext, input: OrderInput): Promise<string> {
  const orderId = input?.orderId ?? "order-demo";

  const amount = await ctx.activity("charge", () => {
    const total = incrementCharge();
    console.log(`  [charge] charged ${orderId} (charge count is now ${total})`);
    return total;
  });
  maybeCrash("charge");

  await ctx.activity("sendEmail", () => {
    console.log(`  [sendEmail] confirmation email sent for ${orderId}`);
    return "email-sent";
  });
  maybeCrash("sendEmail");

  await ctx.activity("followUp", () => {
    console.log(`  [followUp] follow-up sent for ${orderId}`);
    return "followup-sent";
  });
  maybeCrash("followUp");

  return `order ${orderId} complete (charged ${amount} time(s))`;
}
