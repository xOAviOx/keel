import { buildRuntime, runWorker } from "./index";
import { closeSharedDb } from "./db";
import type { EventRow } from "./eventLog";

/**
 * cli.ts — start, inspect, and recover workflows from the terminal.
 *
 *   start <name> [jsonInput]      kick off a workflow (runs to first stop)
 *   recover                       one-shot: re-run all interrupted ('running') runs
 *   signal <id> <name> [jsonVal]  deliver an external signal to a run
 *   history <workflow_id>         pretty-print the ordered event log
 *   status <workflow_id>          current status + wake_at
 *   list                          all workflows with status
 *
 * (The long-running worker with the scheduler loop is `npm run worker`.)
 */

function shortPayload(e: EventRow): string {
  const p: any = e.payload ?? {};
  switch (e.type) {
    case "WORKFLOW_STARTED":
      return `name=${p.name}`;
    case "ACTIVITY_COMPLETED":
      return `${p.name ?? ""} -> ${JSON.stringify(p.value)}`;
    case "ACTIVITY_FAILED":
      return `${p.activity ?? ""} !! ${p.name ?? "Error"}: ${p.message ?? ""}`;
    case "TIMER_STARTED":
      return `fireAt=${new Date(p.fireAt).toISOString()}`;
    case "TIMER_FIRED":
      return `fired`;
    case "SIGNAL_RECEIVED":
      return `${p.signalName} <- ${JSON.stringify(p.value)}`;
    case "SIGNAL_CONSUMED":
      return `${p.signalName} (#${p.signalId}) -> ${JSON.stringify(p.value)}`;
    case "WORKFLOW_COMPLETED":
      return `result=${JSON.stringify(p.result)}`;
    case "WORKFLOW_FAILED":
      return `${p.name ?? "Error"}: ${p.message ?? ""}`;
    default:
      return JSON.stringify(p);
  }
}

async function main(): Promise<void> {
  const [command, ...args] = process.argv.slice(2);

  // The worker is long-running (recovery + scheduler loop); it manages its own
  // lifecycle and never closes the shared DB, so handle it before the switch.
  if (command === "worker") {
    await runWorker();
    return;
  }

  const { runtime } = buildRuntime();

  switch (command) {
    case "start": {
      const name = args[0];
      if (!name) throw new Error("usage: start <name> [jsonInput]");
      const input = args[1] ? JSON.parse(args[1]) : {};
      const id = runtime.createRun(name, input);
      console.log(`started ${name} -> ${id}`);
      await runtime.runWorkflow(id); // may hard-crash via CRASH_AFTER
      const st = runtime.getState(id);
      console.log(`status: ${st?.status}${st?.wake_at ? ` (wake_at ${new Date(st.wake_at).toISOString()})` : ""}`);
      console.log(`workflow id: ${id}`);
      break;
    }

    case "recover": {
      const recovered = await runtime.recover();
      console.log(`recovered ${recovered.length} workflow(s): ${recovered.join(", ") || "(none)"}`);
      break;
    }

    case "signal": {
      const id = args[0];
      const signalName = args[1];
      if (!id || !signalName) throw new Error("usage: signal <workflow_id> <name> [jsonValue]");
      const value = args[2] ? JSON.parse(args[2]) : null;
      const { signalId, status } = await runtime.deliverSignal(id, signalName, value);
      console.log(`signaled ${signalName} (#${signalId}) -> ${id}`);
      console.log(`status: ${status}`);
      break;
    }

    case "history": {
      const id = args[0];
      if (!id) throw new Error("usage: history <workflow_id>");
      const events = runtime.log.getEvents(id);
      if (events.length === 0) {
        console.log(`(no events for ${id})`);
        break;
      }
      console.log(`history for ${id}:`);
      for (const e of events) {
        const ts = new Date(e.created_at).toISOString();
        const seq = e.seq < 0 ? "  -" : String(e.seq).padStart(3, " ");
        console.log(`  #${String(e.id).padStart(3, " ")} seq=${seq} ${e.type.padEnd(18)} ${shortPayload(e)}  @${ts}`);
      }
      break;
    }

    case "status": {
      const id = args[0];
      if (!id) throw new Error("usage: status <workflow_id>");
      const st = runtime.getState(id);
      if (!st) {
        console.log(`(no such workflow ${id})`);
        break;
      }
      console.log(`${id}`);
      console.log(`  name:    ${st.name}`);
      console.log(`  status:  ${st.status}`);
      console.log(`  wake_at: ${st.wake_at ? new Date(st.wake_at).toISOString() : "-"}`);
      break;
    }

    case "list": {
      const rows = runtime.listWorkflows();
      if (rows.length === 0) {
        console.log("(no workflows)");
        break;
      }
      for (const r of rows) {
        const wake = r.wake_at ? ` wake_at=${new Date(r.wake_at).toISOString()}` : "";
        console.log(`${r.workflow_id}  ${r.name.padEnd(12)} ${r.status.padEnd(10)}${wake}`);
      }
      break;
    }

    default:
      console.log(
        [
          "usage:",
          "  start <name> [jsonInput]      kick off a workflow",
          "  worker                        recovery + scheduler loop (long-running)",
          "  recover                       one-shot: re-run interrupted runs",
          "  signal <id> <name> [jsonVal]  deliver an external signal to a run",
          "  history <workflow_id>         ordered event log",
          "  status <workflow_id>          status + wake_at",
          "  list                          all workflows",
        ].join("\n"),
      );
  }

  closeSharedDb();
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});
