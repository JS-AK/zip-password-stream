import { readdirSync, readFileSync, writeFileSync, statSync } from "node:fs";
import { join } from "node:path";

const root = join("build", "cjs");
const needle = "import.meta.url";
const replacement =
  '(typeof document === "undefined" ? require("url").pathToFileURL(__filename).href : "")';

function walk(dir) {
  for (const name of readdirSync(dir)) {
    const full = join(dir, name);
    if (statSync(full).isDirectory()) {
      walk(full);
      continue;
    }
    if (!name.endsWith(".js")) continue;
    const src = readFileSync(full, "utf8");
    if (!src.includes(needle)) continue;
    writeFileSync(full, src.split(needle).join(replacement), "utf8");
  }
}

walk(root);
