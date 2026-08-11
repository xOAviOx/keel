import { describe, it, expect } from "vitest";
import { openDb } from "../src/db";
import { Registry } from "../src/registry";
import { Runtime } from "../src/runtime";
import { WorkflowContext } from "../src/context";

describe("Phase 3: runtime + crash recovery", () => {
  it("runs a workflow to completion with a clean event history", async () => {
    const db = openDb(":memory:");
    const registry = new Registry();
    const counters = { a: 0, b: 0, c: 0 };
    registry.register("order", async (ctx: WorkflowContext) => {
      const a = await ctx.activity("a", () => { counters.a++; return "A"; });
      const b = await ctx.activity("b", () => { counters.b++; return "B"; });
      const c = await ctx.activity("c", () => { counters.c++; return "C"; });
      return a + b + c;
    });

    const rt = new Runtime(db, registry);
    const id = await rt.startWorkflow("order", {});

    expect(rt.getState(id)!.status).toBe("completed");
    expect(counters).toEqual({ a: 1, b: 1, c: 1 });

    const types = rt.log.getEvents(id).map((e) => e.type);
    expect(types).toEqual([
      "WORKFLOW_STARTED",
      "ACTIVITY_COMPLETED",
      "ACTIVITY_COMPLETED",
      "ACTIVITY_COMPLETED",
      "WORKFLOW_COMPLETED",
    ]);
  });

  it("recovers an interrupted run without re-running completed activities", async () => {
    const db = openDb(":memory:");
    const registry = new Registry();
    const counters = { a: 0, b: 0, c: 0 };

    // A single workflow whose body throws after activity 2 while 'crashArmed'.
    // We simulate a HARD crash by invoking the body directly (bypassing the
    // runtime), so workflow_state stays 'running' and no completion is written —
    // exactly the on-disk state a kill -9 mid-run would leave behind.
    let crashArmed = true;
    registry.register("order", async (ctx: WorkflowContext) => {
      await ctx.activity("a", () => { counters.a++; return "A"; });
      await ctx.activity("b", () => { counters.b++; return "B"; });
      if (crashArmed) throw new Error("simulated hard crash after activity 2");
      await ctx.activity("c", () => { counters.c++; return "C"; });
      return "done";
    });

    const rt = new Runtime(db, registry);
    const id = rt.createRun("order", {});

    // Simulate the crash: run the body directly, swallow the throw. State stays 'running'.
    await expect(
      registry.get("order")(new WorkflowContext(id, rt.log), {}),
    ).rejects.toThrow("simulated hard crash");

    expect(rt.getState(id)!.status).toBe("running");
    expect(counters).toEqual({ a: 1, b: 1, c: 0 });

    // Now "restart": recovery re-runs the workflow. Activity 3 should run; 1 & 2 must NOT.
    crashArmed = false;
    const recovered = await rt.recover();

    expect(recovered).toEqual([id]);
    expect(counters).toEqual({ a: 1, b: 1, c: 1 }); // a,b replayed (not re-run); c ran once
    expect(rt.getState(id)!.status).toBe("completed");
  });

  it("marks a workflow failed when it throws a non-suspend error via the runtime", async () => {
    const db = openDb(":memory:");
    const registry = new Registry();
    registry.register("boom", async (ctx: WorkflowContext) => {
      await ctx.activity("explode", () => { throw new Error("kaboom"); });
      return "unreachable";
    });
    const rt = new Runtime(db, registry);
    const id = await rt.startWorkflow("boom", {});
    expect(rt.getState(id)!.status).toBe("failed");
    const types = rt.log.getEvents(id).map((e) => e.type);
    expect(types).toContain("ACTIVITY_FAILED");
    expect(types).toContain("WORKFLOW_FAILED");
  });
});
