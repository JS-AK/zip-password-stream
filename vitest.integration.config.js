import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["build/esm/**/*.test.js"],
    setupFiles: ["./vitest.setup.js"],
  },
});
