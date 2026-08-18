import { Readable, Writable } from "node:stream";
import { mkdtemp, rm } from "node:fs/promises";
import { createWriteStream } from "node:fs";
import { finished } from "node:stream/promises";
import os from "node:os";
import path from "node:path";
import { randomBytes } from "node:crypto";

import { describe, expect, it } from "vitest";

import { Zip, unzipEncrypted, zipEncrypted } from "../lib/index.js";
import { findPython, runPython } from "./test-support/python.js";
import { TINY_PDF } from "./test-support/write-zip.js";
import { ZIP_FLAG_ENCRYPTED } from "../lib/zip/constants.js";

const hasPython = findPython() !== null;

/**
 * Our writer always emits bit 3 with zero local sizes, so every round-trip goes
 * through the reader's unknown-length scan. The budget is a guard against that
 * scan re-inflating the whole accumulator per byte (minutes for this body), not a
 * micro-benchmark — keep it.
 */
const ROUND_TRIP_BUDGET_MS = 5000;
/** Room for the budget assertion to be reported instead of killing the run. */
const ROUND_TRIP_TIMEOUT_MS = 20_000;
/** Random bytes do not deflate, so the compressed body stays this big. */
const LARGE_BODY_SIZE = 256 * 1024;

const PYTHON_READ_ZIP = `
import sys, zipfile
path, pwd = sys.argv[1], sys.argv[2].encode()
with zipfile.ZipFile(path) as zf:
    assert zf.read("a.txt", pwd=pwd) == b"hello\\n"
print("ok")
`;

function collectWritable(): { dest: Writable; toBuffer: () => Buffer } {
  const chunks: Buffer[] = [];
  const dest = new Writable({
    write(chunk: Buffer, _encoding, callback): void {
      chunks.push(Buffer.from(chunk));
      callback();
    },
  });

  return {
    dest,
    toBuffer(): Buffer {
      return Buffer.concat(chunks);
    },
  };
}

async function collect(entry: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];

  for await (const chunk of entry) {
    chunks.push(Buffer.from(chunk));
  }

  return Buffer.concat(chunks);
}

async function addTwoFiles(zipWriter: {
  add(name: string, source: Readable): Promise<void>;
}): Promise<void> {
  await zipWriter.add("a.txt", Readable.from([Buffer.from("hello\n")]));
  await zipWriter.add("nested/b.pdf", Readable.from([TINY_PDF]));
}

describe("zipEncrypted", () => {
  it("round-trips unencrypted add + end through Zip.open", async () => {
    const { dest, toBuffer } = collectWritable();
    const zipWriter = zipEncrypted(dest);

    await addTwoFiles(zipWriter);
    await zipWriter.end();

    const zip = toBuffer();
    const seen: { data: Buffer; path: string }[] = [];

    for await (const entry of Zip.open(Readable.from([zip]))) {
      seen.push({ data: await collect(entry), path: entry.path });
    }

    expect(seen.map((e) => e.path)).toEqual(["a.txt", "nested/b.pdf"]);
    expect(seen[0]?.data.equals(Buffer.from("hello\n"))).toBe(true);
    expect(seen[1]?.data.equals(TINY_PDF)).toBe(true);
  });

  it("round-trips ZipCrypto with the password and rejects a wrong one", async () => {
    const { dest, toBuffer } = collectWritable();
    const zipWriter = zipEncrypted(dest, { password: "secret" });

    await addTwoFiles(zipWriter);
    await zipWriter.end();

    const zip = toBuffer();
    const seen: { data: Buffer; path: string }[] = [];

    for await (const entry of Zip.open(Readable.from([zip])).password("secret")) {
      seen.push({ data: await collect(entry), path: entry.path });
    }

    expect(seen.map((e) => e.path)).toEqual(["a.txt", "nested/b.pdf"]);
    expect(seen[0]?.data.equals(Buffer.from("hello\n"))).toBe(true);
    expect(seen[1]?.data.equals(TINY_PDF)).toBe(true);

    await expect(
      (async () => {
        for await (const entry of Zip.open(Readable.from([zip])).password("wrong")) {
          await collect(entry);
        }
      })(),
    ).rejects.toThrow(/invalid zip password/);
  });

  it(
    "round-trips a large incompressible body through Zip.open",
    { timeout: ROUND_TRIP_TIMEOUT_MS },
    async () => {
      const body = randomBytes(LARGE_BODY_SIZE);
      const { dest, toBuffer } = collectWritable();
      const zipWriter = zipEncrypted(dest);

      await zipWriter.add("big.bin", Readable.from([body]));
      await zipWriter.end();

      const zip = toBuffer();
      const started = performance.now();
      const seen: { data: Buffer; path: string }[] = [];

      for await (const entry of Zip.open(Readable.from([zip]))) {
        seen.push({ data: await collect(entry), path: entry.path });
      }

      expect(seen.map((e) => e.path)).toEqual(["big.bin"]);
      expect(seen[0]?.data.equals(body)).toBe(true);
      expect(performance.now() - started).toBeLessThan(ROUND_TRIP_BUDGET_MS);
    },
  );

  it(
    "round-trips a large incompressible ZipCrypto body through Zip.open",
    { timeout: ROUND_TRIP_TIMEOUT_MS },
    async () => {
      const body = randomBytes(LARGE_BODY_SIZE);
      const { dest, toBuffer } = collectWritable();
      const zipWriter = zipEncrypted(dest, { password: "secret" });

      await zipWriter.add("big.bin", Readable.from([body]));
      await zipWriter.end();

      const zip = toBuffer();
      const started = performance.now();
      const seen: { data: Buffer; path: string }[] = [];

      for await (const entry of Zip.open(Readable.from([zip])).password("secret")) {
        seen.push({ data: await collect(entry), path: entry.path });
      }

      expect(seen.map((e) => e.path)).toEqual(["big.bin"]);
      expect(seen[0]?.data.equals(body)).toBe(true);
      expect(performance.now() - started).toBeLessThan(ROUND_TRIP_BUDGET_MS);
    },
  );

  it("treats an empty-string password as a real ZipCrypto password", async () => {
    const { dest, toBuffer } = collectWritable();
    const zipWriter = zipEncrypted(dest, { password: "" });

    await zipWriter.add("a.txt", Readable.from([Buffer.from("hello\n")]));
    await zipWriter.end();

    const zip = toBuffer();

    expect(zip.readUInt16LE(6) & ZIP_FLAG_ENCRYPTED).toBe(ZIP_FLAG_ENCRYPTED);

    await expect(
      (async () => {
        for await (const entry of unzipEncrypted(Readable.from([zip]), {})) {
          await collect(entry);
        }
      })(),
    ).rejects.toThrow(/password required/);

    const seen: { data: Buffer; path: string }[] = [];

    for await (const entry of unzipEncrypted(Readable.from([zip]), { password: "" })) {
      seen.push({ data: await collect(entry), path: entry.path });
    }

    expect(seen).toHaveLength(1);
    expect(seen[0]?.path).toBe("a.txt");
    expect(seen[0]?.data.equals(Buffer.from("hello\n"))).toBe(true);

    await expect(
      (async () => {
        for await (const entry of unzipEncrypted(Readable.from([zip]), { password: "wrong" })) {
          await collect(entry);
        }
      })(),
    ).rejects.toThrow(/invalid zip password/);
  });

  it("throws when end() is called twice", async () => {
    const { dest } = collectWritable();
    const zipWriter = zipEncrypted(dest);

    await zipWriter.end();

    await expect(zipWriter.end()).rejects.toThrow(/zip already closed/);
  });

  it("throws when add() is called after end()", async () => {
    const { dest } = collectWritable();
    const zipWriter = zipEncrypted(dest);

    await zipWriter.end();

    await expect(zipWriter.add("a.txt", Readable.from([Buffer.from("hello\n")]))).rejects.toThrow(
      /zip already closed/,
    );
  });

  it("throws when an added body exceeds maxEntrySize", async () => {
    const { dest } = collectWritable();
    const zipWriter = zipEncrypted(dest, { maxEntrySize: 4 });

    await expect(zipWriter.add("a.txt", Readable.from([Buffer.from("hello")]))).rejects.toThrow(
      /entry exceeds maxEntrySize/,
    );
  });

  it.each(["..", ".", ""] as const)("throws for unsafe entry name %j", async (name) => {
    const { dest } = collectWritable();
    const zipWriter = zipEncrypted(dest);

    await expect(zipWriter.add(name, Readable.from([Buffer.from("x")]))).rejects.toThrow(
      /unsafe entry name/,
    );
  });

  it.each(["../../etc/passwd", "/etc/passwd", "C:\\Windows\\evil.dll", "foo/../passwd"] as const)(
    "throws for a traversal or absolute entry name %j",
    async (name) => {
      const { dest } = collectWritable();
      const zipWriter = zipEncrypted(dest);

      await expect(zipWriter.add(name, Readable.from([Buffer.from("x")]))).rejects.toThrow(
        /unsafe entry name/,
      );
    },
  );

  it("rejects a second add while the first is still running", async () => {
    const { dest } = collectWritable();
    const zipWriter = zipEncrypted(dest);
    const hanging = new Readable({
      read(): void {
        /* pushed below */
      },
    });
    const first = zipWriter.add("a.txt", hanging);

    await expect(zipWriter.add("b.txt", Readable.from([Buffer.from("b")]))).rejects.toThrow(
      /zip write in progress/,
    );
    await expect(zipWriter.end()).rejects.toThrow(/zip write in progress/);

    hanging.push(Buffer.from("hello\n"));
    hanging.push(null);
    await first;
    await zipWriter.end();
  });

  it("rejects add and end after a failed add", async () => {
    const { dest } = collectWritable();
    const zipWriter = zipEncrypted(dest, { maxEntrySize: 4 });

    await expect(zipWriter.add("a.txt", Readable.from([Buffer.from("hello")]))).rejects.toThrow(
      /entry exceeds maxEntrySize/,
    );
    await expect(zipWriter.add("b.txt", Readable.from([Buffer.from("ok")]))).rejects.toThrow(
      /zip writer failed/,
    );
    await expect(zipWriter.end()).rejects.toThrow(/zip writer failed/);
  });

  it("rejects when the destination closes instead of draining", { timeout: 2000 }, async () => {
    const dest = new Writable({
      highWaterMark: 1,
      write(_chunk: Buffer, _encoding, _callback): void {
        this.destroy();
      },
    });

    dest.on("error", () => {
      /* destroy without Error still closes the writable */
    });
    const zipWriter = zipEncrypted(dest);

    await expect(zipWriter.add("a.txt", Readable.from([Buffer.from("hello\n")]))).rejects.toThrow(
      /zip destination closed/,
    );
  });

  it("does not resolve end() until dest has finished", async () => {
    let flushed = false;
    const dest = new Writable({
      final(callback): void {
        setTimeout(() => {
          flushed = true;
          callback();
        }, 40);
      },
      write(_chunk: Buffer, _encoding, callback): void {
        callback();
      },
    });
    const zipWriter = zipEncrypted(dest);

    await zipWriter.end();

    expect(flushed).toBe(true);
  });
});

// CPython zipfile reads ZipCrypto; this checks our writer, not our parser.
describe.skipIf(!hasPython)("interop: archive written for CPython zipfile", () => {
  it("is readable by Python zipfile with the ZipCrypto password", async () => {
    const dir = await mkdtemp(path.join(os.tmpdir(), "zip-write-"));
    const zipPath = path.join(dir, "out.zip");

    try {
      const dest = createWriteStream(zipPath);
      const zipWriter = zipEncrypted(dest, { password: "secret" });

      await zipWriter.add("a.txt", Readable.from([Buffer.from("hello\n")]));
      await zipWriter.end();
      await finished(dest);

      const result = runPython(PYTHON_READ_ZIP, [zipPath, "secret"]);

      expect(result.status, result.stderr).toBe(0);
    } finally {
      await rm(dir, { force: true, recursive: true });
    }
  });
});
