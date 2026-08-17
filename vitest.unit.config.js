import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    coverage: {
      exclude: ["src/**/*.unit.spec.ts", "src/lib/unzip.ts"],
      include: ["src/lib/**/*.ts"],
      provider: "v8",
      reporter: ["text", "lcov"],
      thresholds: {
        branches: 50,
        functions: 75,
        lines: 65,
        statements: 65,
      },
    },
    environment: "node",
    include: ["src/**/*.unit.spec.ts"],
    setupFiles: ["./vitest.setup.js"],
  },
});
