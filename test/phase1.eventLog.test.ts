import { describe, it, expect } from "vitest";
import { openDb } from "../src/db";
import { EventLog } from "../src/eventLog";

describe("Phase 1: append-only event log", () => {
  it("appends events in order and reads them back by id ASC", () => {
    const log = new EventLog(openDb(":memory:"));
    const wf = "wf-1";

    const id0 = log.append(wf, 0, "WORKFLOW_STARTED", { name: "demo", input: 1 });
    const id1 = log.append(wf, 0, "ACTIVITY_COMPLETED", { value: "a", name: "charge" });
    const id2 = log.append(wf, 1, "ACTIVITY_COMPLETED", { value: "b", name: "email" });

    expect(id0).toBeLessThan(id1);
    expect(id1).toBeLessThan(id2);

    const events = log.getEvents(wf);
    expect(events.map((e) => e.id)).toEqual([id0, id1, id2]);
    expect(events.map((e) => e.type)).toEqual([
      "WORKFLOW_STARTED",
      "ACTIVITY_COMPLETED",
      "ACTIVITY_COMPLETED",
    ]);
    expect((events[1]!.payload as any).value).toBe("a");
  });

  it("findEvent returns the right (seq,type) and undefined for a missing seq", () => {
    const log = new EventLog(openDb(":memory:"));
    const wf = "wf-2";
    log.append(wf, 0, "ACTIVITY_COMPLETED", { value: 42 });
    log.append(wf, 1, "ACTIVITY_COMPLETED", { value: 99 });

    expect((log.findEvent(wf, 1, "ACTIVITY_COMPLETED")!.payload as any).value).toBe(99);
    expect(log.findEvent(wf, 2, "ACTIVITY_COMPLETED")).toBeUndefined();
    // wrong type at an existing seq is also a miss
    expect(log.findEvent(wf, 0, "ACTIVITY_FAILED")).toBeUndefined();
  });

  it("round-trips null and undefined-wrapped payloads", () => {
    const log = new EventLog(openDb(":memory:"));
    const wf = "wf-3";
    log.append(wf, 0, "ACTIVITY_COMPLETED", { value: null });
    log.append(wf, 1, "ACTIVITY_COMPLETED", { value: undefined });

    const e0 = log.findEvent(wf, 0, "ACTIVITY_COMPLETED")!;
    const e1 = log.findEvent(wf, 1, "ACTIVITY_COMPLETED")!;
    expect((e0.payload as any).value).toBeNull();
    // undefined does not survive JSON; the wrapper key is simply absent -> undefined
    expect((e1.payload as any).value).toBeUndefined();
  });

  it("isolates events by workflow_id", () => {
    const db = openDb(":memory:");
    const log = new EventLog(db);
    log.append("A", 0, "WORKFLOW_STARTED", {});
    log.append("B", 0, "WORKFLOW_STARTED", {});
    expect(log.getEvents("A")).toHaveLength(1);
    expect(log.getEvents("B")).toHaveLength(1);
  });
});
