import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { getDb, type DB } from "./db";
import { Registry } from "./registry";
import { Runtime } from "./runtime";
import { Scheduler } from "./scheduler";
import { orderFlow } from "./workflows/orderFlow";
import { sleepFlow } from "./workflows/sleepFlow";
import { approvalFlow } from "./workflows/approvalFlow";

/**
 * index.ts — wiring + the long-running worker entrypoint.
 *
 * `buildRuntime` constructs the Registry (all known workflows) + Runtime over a
 * given DB. Both the CLI and the tests use it so everything shares one registry.
 * The Scheduler is wired into runWorker in Phase 6.
 */

export function registerWorkflows(registry: Registry): Registry {
  registry.register("orderFlow", orderFlow);
  registry.register("sleepFlow", sleepFlow);
  registry.register("approvalFlow", approvalFlow);
  return registry;
}

export function buildRuntime(db: DB = getDb()): {
  db: DB;
  registry: Registry;
  runtime: Runtime;
} {
  const registry = registerWorkflows(new Registry());
  const runtime = new Runtime(db, registry);
  return { db, registry, runtime };
}

/**
 * The worker: on boot, recover interrupted runs, then poll for due timers on a
 * background loop. This is the long-running process (`npm run worker`).
 */
export async function runWorker(): Promise<void> {
  const { runtime } = buildRuntime();
  console.log("[worker] booting; recovering interrupted workflows…");
  const recovered = await runtime.recover();
  console.log(`[worker] recovered ${recovered.length} workflow(s): ${recovered.join(", ") || "(none)"}`);

  const scheduler = new Scheduler(runtime);
  // Fire anything already due right now, then start the poll loop.
  await scheduler.tick();
  scheduler.start();
  console.log("[worker] scheduler running; watching for due timers. Ctrl-C to stop.");

  // Keep the process alive (scheduler interval is unref'd for tests) and shut
  // down cleanly on signals.
  const keepAlive = setInterval(() => {}, 1 << 30);
  const shutdown = () => {
    console.log("\n[worker] shutting down…");
    scheduler.stop();
    clearInterval(keepAlive);
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

// Run the worker only when this file is the direct entrypoint.
const isMain =
  process.argv[1] !== undefined &&
  resolve(process.argv[1]) === resolve(fileURLToPath(import.meta.url));
if (isMain) {
  runWorker().catch((err) => {
    console.error("[worker] fatal:", err);
    process.exit(1);
  });
}
