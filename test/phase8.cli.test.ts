import { describe, it, expect, beforeEach } from "vitest";
import { spawn } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

function run(args: string[], env: Record<string, string>): Promise<{ code: number | null; stdout: string }> {
  return new Promise((resolveP) => {
    const child = spawn(process.execPath, ["--import", "tsx", "src/cli.ts", ...args], {
      cwd: process.cwd(),
      env: { ...process.env, ...env },
    });
    let stdout = "";
    child.stdout.on("data", (d) => (stdout += d.toString()));
    child.on("close", (code) => resolveP({ code, stdout }));
  });
}

describe("Phase 8: observability CLI", () => {
  let ENGINE_DB: string;
  let CHARGE_FILE: string;

  beforeEach(() => {
    const dir = mkdtempSync(join(tmpdir(), "durable-p8-"));
    ENGINE_DB = join(dir, "engine.db");
    CHARGE_FILE = join(dir, "charges.txt");
  });

  it("start/history/status/list produce readable output for a known workflow", async () => {
    const env = { ENGINE_DB, CHARGE_FILE };

    const started = await run(["start", "orderFlow", JSON.stringify({ orderId: "cli-1" })], env);
    expect(started.code).toBe(0);
    const m = started.stdout.match(/-> ([0-9a-f-]{36})/);
    expect(m).toBeTruthy();
    const id = m![1]!;
    expect(started.stdout).toContain("status: completed");

    const history = await run(["history", id], env);
    expect(history.stdout).toContain("WORKFLOW_STARTED");
    expect(history.stdout).toContain("ACTIVITY_COMPLETED");
    expect(history.stdout).toContain("WORKFLOW_COMPLETED");
    expect(history.stdout).toContain("charge");

    const status = await run(["status", id], env);
    expect(status.stdout).toContain("status:  completed");
    expect(status.stdout).toContain("orderFlow");

    const list = await run(["list"], env);
    expect(list.stdout).toContain(id);
    expect(list.stdout).toContain("completed");
  });

  it("prints usage for an unknown command", async () => {
    const out = await run(["frobnicate"], { ENGINE_DB, CHARGE_FILE });
    expect(out.stdout).toContain("usage:");
    expect(out.stdout).toContain("worker");
  });
});
