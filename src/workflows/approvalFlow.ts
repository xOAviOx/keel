import type { WorkflowContext } from "../context";

/**
 * approvalFlow.ts — the demo workflow for durable SIGNALS (human-in-the-loop).
 *
 * Steps:
 *   submit()                 -> records the doc as submitted (an activity).
 *   waitForSignal("approve") -> SUSPENDS the run in 'waiting' until an external
 *                               approval signal is delivered. The process can be
 *                               gone for hours/days in between — the wait is
 *                               durable, not an in-memory promise.
 *   finalize()               -> applies the decision (an activity).
 *
 * Deliver the signal from the terminal:
 *   npm run cli signal <workflow_id> approve '{"approved":true,"by":"alice"}'
 *
 * The whole point: a workflow can block on an external, human-paced event and
 * survive a full restart while blocked, then resume exactly where it left off.
 */
export interface ApprovalInput {
  docId?: string;
}

export interface ApprovalDecision {
  approved: boolean;
  by?: string;
}

export async function approvalFlow(ctx: WorkflowContext, input: ApprovalInput): Promise<string> {
  const docId = input?.docId ?? "doc-demo";

  await ctx.activity("submit", () => {
    console.log(`  [submit] ${docId} submitted; waiting for approval…`);
    return "submitted";
  });

  // Durable block: parks as 'waiting' until someone sends the 'approve' signal.
  const decision = await ctx.waitForSignal<ApprovalDecision>("approve");

  await ctx.activity("finalize", () => {
    const verdict = decision.approved ? "APPROVED" : "REJECTED";
    const who = decision.by ? ` by ${decision.by}` : "";
    console.log(`  [finalize] ${docId} ${verdict}${who}`);
    return decision.approved ? "approved" : "rejected";
  });

  return `${docId}: ${decision.approved ? "approved" : "rejected"}`;
}
