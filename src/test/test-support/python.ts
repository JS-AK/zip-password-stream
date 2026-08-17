import { type SpawnSyncReturns, spawnSync } from "node:child_process";

type Python = { base: string[]; cmd: string };

const CANDIDATES: Python[] = [
  { base: ["-3"], cmd: "py" },
  { base: [], cmd: "python3" },
  { base: [], cmd: "python" },
];

let cached: Python | null | undefined;

/** Locate a working CPython, or null. Windows Store stubs fail the probe. */
export function findPython(): Python | null {
  if (cached !== undefined) {
    return cached;
  }
  for (const candidate of CANDIDATES) {
    const probe = spawnSync(candidate.cmd, [...candidate.base, "-c", "print('ok')"], {
      encoding: "utf8",
    });

    if (!probe.error && probe.status === 0 && probe.stdout.includes("ok")) {
      cached = candidate;

      return cached;
    }
  }
  cached = null;

  return cached;
}

function argv(script: string, args: string[]): [string, string[]] {
  const python = findPython();

  if (!python) {
    throw new Error("python not available");
  }

  return [python.cmd, [...python.base, "-c", script, ...args]];
}

/** Run a short CPython snippet; stdout is decoded as utf8. */
export function runPython(script: string, args: string[] = []): SpawnSyncReturns<string> {
  const [cmd, spawnArgs] = argv(script, args);

  return spawnSync(cmd, spawnArgs, { encoding: "utf8" });
}

/** For scripts writing raw bytes to stdout, where utf8 decoding would corrupt them. */
export function runPythonBinary(script: string, args: string[] = []): SpawnSyncReturns<Buffer> {
  const [cmd, spawnArgs] = argv(script, args);

  return spawnSync(cmd, spawnArgs);
}
