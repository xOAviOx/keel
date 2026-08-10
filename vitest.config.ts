import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Each test file spins up its own SQLite file; run files serially-ish but
    // isolate per-file to avoid cross-test DB contention on shared paths.
    include: ["test/**/*.test.ts"],
    // Give child-process crash/recovery tests room to breathe.
    testTimeout: 30_000,
    hookTimeout: 30_000,
    // Avoid parallel workers stomping on the same on-disk demo DB.
    fileParallelism: false,
  },
});
