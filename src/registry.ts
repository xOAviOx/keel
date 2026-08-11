import type { WorkflowContext } from "./context";

/**
 * registry.ts — maps a workflow NAME to its function. The name is what we
 * persist in workflow_state; on recovery we look the function back up by name
 * and replay it. Workflow functions must be pure orchestration (see the
 * determinism rules in context.ts).
 */
export type WorkflowFn<Input = any, Output = any> = (
  ctx: WorkflowContext,
  input: Input,
) => Promise<Output>;

export class Registry {
  private readonly map = new Map<string, WorkflowFn>();

  register<I, O>(name: string, fn: WorkflowFn<I, O>): this {
    if (this.map.has(name)) {
      throw new Error(`Workflow '${name}' is already registered`);
    }
    this.map.set(name, fn as WorkflowFn);
    return this;
  }

  has(name: string): boolean {
    return this.map.has(name);
  }

  get(name: string): WorkflowFn {
    const fn = this.map.get(name);
    if (!fn) throw new Error(`No workflow registered under '${name}'`);
    return fn;
  }

  names(): string[] {
    return [...this.map.keys()];
  }
}
