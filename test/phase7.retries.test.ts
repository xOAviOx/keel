import { describe, it, expect } from "vitest";
import { openDb } from "../src/db";
import { Registry } from "../src/registry";
import { Runtime } from "../src/runtime";
import type { WorkflowContext } from "../src/context";

describe("Phase 7: retries with backoff + terminal failures", () => {
  it("a flaky activity (fails twice then succeeds) logs ONE ACTIVITY_COMPLETED", async () => {
    const db = openDb(":memory:");
    const registry = new Registry();
    let attempts = 0;

    registry.register("flaky", async (ctx: WorkflowContext) => {
      return ctx.activity(
        "flakyStep",
        () => {
          attempts++;
          if (attempts < 3) throw new Error(`transient #${attempts}`);
          return "eventually-ok";
        },
        { retries: 3, backoffMs: 1 },
      );
    });

    const rt = new Runtime(db, registry);
    const id = await rt.startWorkflow("flaky", {});

    expect(attempts).toBe(3); // 2 failures + 1 success
    expect(rt.getState(id)!.status).toBe("completed");

    const events = rt.log.getEvents(id);
    expect(events.filter((e) => e.type === "ACTIVITY_COMPLETED")).toHaveLength(1);
    expect(events.filter((e) => e.type === "ACTIVITY_FAILED")).toHaveLength(0);
    expect((events.find((e) => e.type === "ACTIVITY_COMPLETED")!.payload as any).value).toBe(
      "eventually-ok",
    );
  });

  it("an always-failing activity ends the workflow 'failed' exactly once", async () => {
    const db = openDb(":memory:");
    const registry = new Registry();
    let attempts = 0;

    registry.register("doomed", async (ctx: WorkflowContext) => {
      await ctx.activity(
        "boom",
        () => {
          attempts++;
          throw new Error("permanent");
        },
        { retries: 2, backoffMs: 1 },
      );
      return "unreachable";
    });

    const rt = new Runtime(db, registry);
    const id = await rt.startWorkflow("doomed", {});

    expect(attempts).toBe(3); // initial + 2 retries
    expect(rt.getState(id)!.status).toBe("failed");

    const events = rt.log.getEvents(id);
    expect(events.filter((e) => e.type === "ACTIVITY_FAILED")).toHaveLength(1);
    expect(events.filter((e) => e.type === "WORKFLOW_FAILED")).toHaveLength(1);
  });

  it("a logged ACTIVITY_FAILED re-throws on replay WITHOUT re-running fn", async () => {
    const db = openDb(":memory:");
    const registry = new Registry();
    let attempts = 0;

    registry.register("doomed2", async (ctx: WorkflowContext) => {
      await ctx.activity("boom", () => { attempts++; throw new Error("permanent"); }, { retries: 1, backoffMs: 1 });
      return "unreachable";
    });

    const rt = new Runtime(db, registry);
    const id = await rt.startWorkflow("doomed2", {});
    expect(rt.getState(id)!.status).toBe("failed");
    const attemptsAfterFirst = attempts; // 2 (initial + 1 retry)
    expect(attemptsAfterFirst).toBe(2);

    // Force a replay of the same run. The logged ACTIVITY_FAILED must re-throw
    // deterministically without invoking fn again.
    rt.setStatus(id, "running", null); // pretend it's resumable
    await rt.runWorkflow(id);

    expect(attempts).toBe(attemptsAfterFirst); // fn NOT re-run on replay
    expect(rt.getState(id)!.status).toBe("failed");
  });
});
