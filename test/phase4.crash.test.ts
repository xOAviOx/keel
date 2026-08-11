import { describe, it, expect, beforeEach } from "vitest";
import { spawn } from "node:child_process";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openDb } from "../src/db";

/**
 * Phase 4 — the proof. A REAL child process is hard-killed (process.exit(1))
 * mid-workflow, right after the charge is committed. A separate recovery
 * process then resumes the run. The on-disk charge counter must read exactly 1.
 */

function run(
  args: string[],
  env: Record<string, string>,
): Promise<{ code: number | null; stdout: string; stderr: string }> {
  return new Promise((resolveP) => {
    const child = spawn(process.execPath, ["--import", "tsx", "src/cli.ts", ...args], {
      cwd: process.cwd(),
      env: { ...process.env, ...env },
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d) => (stdout += d.toString()));
    child.stderr.on("data", (d) => (stderr += d.toString()));
    child.on("close", (code) => resolveP({ code, stdout, stderr }));
  });
}

describe("Phase 4: exactly-once charge across a real process crash", () => {
  let dir: string;
  let ENGINE_DB: string;
  let CHARGE_FILE: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "durable-p4-"));
    ENGINE_DB = join(dir, "engine.db");
    CHARGE_FILE = join(dir, "charges.txt");
  });

  it("charges once, crashes, recovers, and never charges again", async () => {
    // 1) Start with an injected hard crash right after the charge is logged.
    const crashed = await run(["start", "orderFlow", JSON.stringify({ orderId: "X1" })], {
      ENGINE_DB,
      CHARGE_FILE,
      CRASH_AFTER: "charge",
    });
    expect(crashed.code).toBe(1); // process.exit(1)
    expect(readFileSync(CHARGE_FILE, "utf8").trim()).toBe("1"); // charged exactly once

    // 2) Recover in a fresh process. Must resume at sendEmail, NOT re-charge.
    const recovered = await run(["recover"], { ENGINE_DB, CHARGE_FILE });
    expect(recovered.code).toBe(0);

    // 3) The charge counter is STILL 1 — the completed side effect never repeated.
    expect(readFileSync(CHARGE_FILE, "utf8").trim()).toBe("1");

    // 4) The workflow ended 'completed', and only one ACTIVITY_COMPLETED for 'charge'.
    const db = openDb(ENGINE_DB);
    const status = (
      db.prepare(`SELECT status FROM workflow_state LIMIT 1`).get() as { status: string }
    ).status;
    expect(status).toBe("completed");

    const chargeCompletions = db
      .prepare(
        `SELECT COUNT(*) AS n FROM events WHERE type='ACTIVITY_COMPLETED' AND payload LIKE '%"name":"charge"%'`,
      )
      .get() as { n: number };
    expect(chargeCompletions.n).toBe(1);
    db.close();
  });

  it("can crash at a later step too and still charge only once", async () => {
    // Crash AFTER sendEmail this time; charge already happened on step 1.
    const crashed = await run(["start", "orderFlow", JSON.stringify({ orderId: "X2" })], {
      ENGINE_DB,
      CHARGE_FILE,
      CRASH_AFTER: "sendEmail",
    });
    expect(crashed.code).toBe(1);
    expect(readFileSync(CHARGE_FILE, "utf8").trim()).toBe("1");

    const recovered = await run(["recover"], { ENGINE_DB, CHARGE_FILE });
    expect(recovered.code).toBe(0);
    expect(readFileSync(CHARGE_FILE, "utf8").trim()).toBe("1");
  });
});
