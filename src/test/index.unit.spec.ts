import {
	describe, expect, it,
} from "vitest";

import { isTest } from "../lib/index.js";

describe("isTest", () => {
	it("returns true for TEST", () => {
		expect(isTest("TEST")).toBe(true);
	});

	it("returns false for other values", () => {
		expect(isTest("other")).toBe(false);
	});
});
