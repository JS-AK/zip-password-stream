import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		coverage: {
			exclude: [
				"build/esm/index.js",
			],
			include: ["build/esm/lib/**/*.js"],
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
		include: ["build/esm/**/*.unit.spec.js"],
		setupFiles: ["./vitest.setup.js"],
	},
});
