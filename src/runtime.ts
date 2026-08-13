import { randomUUID } from "node:crypto";
import type { DB } from "./db";
import { EventLog } from "./eventLog";
import { WorkflowContext } from "./context";
import { Registry } from "./registry";
import { isWorkflowSuspended, serializeError } from "./errors";

/**
 * runtime.ts — runs and replays workflows, and recovers interrupted runs on boot.
 *
 * Lifecycle events (WORKFLOW_STARTED / COMPLETED / FAILED) are markers, not
 * ctx calls, so we store them at seq = -1 to keep them out of the way of the
 * ctx-call seq space (0,1,2,...). This avoids any collision with activity seqs.
 */
const LIFECYCLE_SEQ = -1;
// External signal deliveries aren't ctx calls, so they live at their own marker
// seq — kept clear of lifecycle (-1) and the ctx-call seq space (0,1,2,...).
const SIGNAL_SEQ = -2;

export type WorkflowStatus = "running" | "sleeping" | "waiting" | "completed" | "failed";

export interface WorkflowStateRow {
  workflow_id: string;
  name: string;
  input: unknown; // parsed JSON
  status: WorkflowStatus;
  wake_at: number | null;
  created_at: number;
  updated_at: number;
}

interface RawStateRow {
  workflow_id: string;
  name: string;
  input: string;
  status: WorkflowStatus;
  wake_at: number | null;
  created_at: number;
  updated_at: number;
}

export class Runtime {
  readonly log: EventLog;

  constructor(
    private readonly db: DB,
    private readonly registry: Registry,
  ) {
    this.log = new EventLog(db);
  }

  // ---------------------------------------------------------------------------
  // Starting runs
  // ---------------------------------------------------------------------------

  /**
   * Create a new run row + WORKFLOW_STARTED event atomically. Returns the id.
   * Does NOT execute the workflow — callers run it via runWorkflow so they can,
   * e.g., print the id before a crashy execution.
   */
  createRun(name: string, input: unknown): string {
    if (!this.registry.has(name)) {
      throw new Error(`Cannot start unknown workflow '${name}'`);
    }
    const workflowId = randomUUID();
    const now = Date.now();

    // Atomic: the run must never exist without its STARTED event, or vice versa.
    this.db.transaction(() => {
      this.db
        .prepare(
          `INSERT INTO workflow_state
             (workflow_id, name, input, status, wake_at, created_at, updated_at)
           VALUES (?, ?, ?, 'running', NULL, ?, ?)`,
        )
        .run(workflowId, name, JSON.stringify(input ?? null), now, now);
      this.log.append(workflowId, LIFECYCLE_SEQ, "WORKFLOW_STARTED", { name, input });
    })();

    return workflowId;
  }

  /** Convenience: create + run to first suspension/completion/failure. */
  async startWorkflow(name: string, input: unknown): Promise<string> {
    const id = this.createRun(name, input);
    await this.runWorkflow(id);
    return id;
  }

  // ---------------------------------------------------------------------------
  // Running / replaying a single run
  // ---------------------------------------------------------------------------

  /**
   * Execute (or replay) a workflow to its next stopping point. Because of the
   * event log, already-completed activities are skipped and execution resumes
   * from the first not-yet-done step.
   */
  async runWorkflow(workflowId: string): Promise<void> {
    const state = this.getState(workflowId);
    if (!state) throw new Error(`No such workflow '${workflowId}'`);
    if (state.status === "completed" || state.status === "failed") {
      return; // terminal; nothing to do
    }

    const fn = this.registry.get(state.name);
    const ctx = new WorkflowContext(workflowId, this.log);

    try {
      const result = await fn(ctx, state.input);
      // Normal completion.
      this.db.transaction(() => {
        this.log.append(workflowId, LIFECYCLE_SEQ, "WORKFLOW_COMPLETED", { result });
        this.setStatus(workflowId, "completed", null);
      })();
    } catch (err) {
      if (isWorkflowSuspended(err)) {
        // The workflow hit a durable wait. Park it (not failed) so it can be
        // resumed later by replaying from the top.
        if (err.reason === "signal") {
          // Waiting on an external signal — no wake time. Delivery (sendSignal)
          // flips it back to 'running' and replays.
          this.setStatus(workflowId, "waiting", null);
        } else {
          // Waiting on a durable timer. The scheduler replays it when due.
          this.setStatus(workflowId, "sleeping", err.wakeAt);
        }
        return;
      }
      // A real, unhandled failure. Mark terminal-failed.
      this.db.transaction(() => {
        this.log.append(workflowId, LIFECYCLE_SEQ, "WORKFLOW_FAILED", serializeError(err));
        this.setStatus(workflowId, "failed", null);
      })();
    }
  }

  // ---------------------------------------------------------------------------
  // Recovery on boot
  // ---------------------------------------------------------------------------

  /**
   * On boot, re-run every workflow that was mid-execution ('running') when the
   * process died. Replay skips completed activities and resumes from the first
   * unfinished step. ('sleeping' runs are the scheduler's job, not recovery's.)
   */
  async recover(): Promise<string[]> {
    const rows = this.db
      .prepare(`SELECT workflow_id FROM workflow_state WHERE status = 'running' ORDER BY created_at ASC`)
      .all() as { workflow_id: string }[];

    const recovered: string[] = [];
    for (const { workflow_id } of rows) {
      await this.runWorkflow(workflow_id);
      recovered.push(workflow_id);
    }
    return recovered;
  }

  // ---------------------------------------------------------------------------
  // State access (also used by the scheduler)
  // ---------------------------------------------------------------------------

  getState(workflowId: string): WorkflowStateRow | undefined {
    const row = this.db
      .prepare(`SELECT * FROM workflow_state WHERE workflow_id = ?`)
      .get(workflowId) as RawStateRow | undefined;
    return row ? parseState(row) : undefined;
  }

  listWorkflows(): WorkflowStateRow[] {
    const rows = this.db
      .prepare(`SELECT * FROM workflow_state ORDER BY created_at ASC`)
      .all() as RawStateRow[];
    return rows.map(parseState);
  }

  // ---------------------------------------------------------------------------
  // Timer support (used by the Scheduler in Phase 6)
  // ---------------------------------------------------------------------------

  /** Workflows that are sleeping and whose wake_at is at or before `now`. */
  dueSleepingWorkflows(now: number = Date.now()): string[] {
    const rows = this.db
      .prepare(
        `SELECT workflow_id FROM workflow_state
         WHERE status = 'sleeping' AND wake_at IS NOT NULL AND wake_at <= ?
         ORDER BY wake_at ASC`,
      )
      .all(now) as { workflow_id: string }[];
    return rows.map((r) => r.workflow_id);
  }

  /**
   * Fire the pending, now-due timer for a sleeping workflow: append TIMER_FIRED
   * for its seq and flip status back to 'running'. Idempotent — if TIMER_FIRED
   * already exists for that seq we don't append a second one. Returns true if a
   * due timer was found (whether or not it had already been fired).
   *
   * The append + status flip happen in ONE transaction so a crash can't leave a
   * fired-but-still-sleeping (or running-but-unfired) state.
   */
  fireDueTimer(workflowId: string, now: number = Date.now()): boolean {
    const events = this.log.getEvents(workflowId);
    const firedSeqs = new Set(
      events.filter((e) => e.type === "TIMER_FIRED").map((e) => e.seq),
    );
    // The workflow is suspended at exactly one sleep; find its unfired timer.
    const pending = events.find(
      (e) =>
        e.type === "TIMER_STARTED" &&
        !firedSeqs.has(e.seq) &&
        (e.payload as { fireAt: number }).fireAt <= now,
    );
    if (!pending) return false;

    this.db.transaction(() => {
      if (!this.log.hasEvent(workflowId, pending.seq, "TIMER_FIRED")) {
        this.log.append(workflowId, pending.seq, "TIMER_FIRED", { firedAt: now });
      }
      this.setStatus(workflowId, "running", null);
    })();
    return true;
  }

  // ---------------------------------------------------------------------------
  // Signal support
  // ---------------------------------------------------------------------------

  /**
   * Durably deliver an external signal to a run: append SIGNAL_RECEIVED and, if
   * the run is parked 'waiting', flip it to 'running' so it becomes eligible for
   * replay. Both happen in ONE transaction, so a crash can't leave a delivered
   * signal without the status flip (or vice versa). Returns the new signal's
   * event id (used to bind it to exactly one wait on replay).
   *
   * A signal delivered to a 'running' or 'sleeping' run is simply BUFFERED in the
   * log — a later waitForSignal(name) will find and consume it. Delivering to a
   * terminal (completed/failed) run is an error.
   *
   * This only persists; it does NOT run the workflow. Use deliverSignal to also
   * advance it (or let boot recovery pick up the now-'running' run).
   */
  sendSignal(workflowId: string, signalName: string, value: unknown): number {
    const state = this.getState(workflowId);
    if (!state) throw new Error(`No such workflow '${workflowId}'`);
    if (state.status === "completed" || state.status === "failed") {
      throw new Error(`Cannot signal '${workflowId}': workflow is ${state.status}`);
    }

    let signalId = 0;
    this.db.transaction(() => {
      signalId = this.log.append(workflowId, SIGNAL_SEQ, "SIGNAL_RECEIVED", { signalName, value });
      if (state.status === "waiting") {
        this.setStatus(workflowId, "running", null);
      }
    })();
    return signalId;
  }

  /**
   * sendSignal + advance. Delivers the signal and, if delivery made the run
   * eligible ('waiting' -> 'running'), replays it now so the wait consumes the
   * signal and execution continues. Returns the signal id and the resulting
   * status. (If the run was 'sleeping', the signal is buffered and consumed
   * after the timer fires; we don't disturb the sleep here.)
   */
  async deliverSignal(
    workflowId: string,
    signalName: string,
    value: unknown,
  ): Promise<{ signalId: number; status: WorkflowStatus }> {
    const signalId = this.sendSignal(workflowId, signalName, value);
    if (this.getState(workflowId)?.status === "running") {
      await this.runWorkflow(workflowId);
    }
    return { signalId, status: this.getState(workflowId)!.status };
  }

  setStatus(workflowId: string, status: WorkflowStatus, wakeAt: number | null): void {
    this.db
      .prepare(
        `UPDATE workflow_state SET status = ?, wake_at = ?, updated_at = ? WHERE workflow_id = ?`,
      )
      .run(status, wakeAt, Date.now(), workflowId);
  }
}

function parseState(row: RawStateRow): WorkflowStateRow {
  return {
    workflow_id: row.workflow_id,
    name: row.name,
    input: JSON.parse(row.input),
    status: row.status,
    wake_at: row.wake_at,
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
}
