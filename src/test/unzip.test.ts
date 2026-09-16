import { Readable, Transform, Writable } from "node:stream";
import { crc32, createDeflateRaw } from "node:zlib";
import { finished, pipeline } from "node:stream/promises";
import { mkdtemp, unlink } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { once } from "node:events";
import os from "node:os";
import path from "node:path";
import { randomBytes } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";

import { describe, expect, it } from "vitest";

import {
  DATA_DESCRIPTOR_WITH_SIG_LEN,
  DD_SIG,
  EOCD,
  EOCD_LEN,
  ZIP_FLAG_DATA_DESCRIPTOR,
  ZIP_FLAG_UTF8,
  ZIP_METHOD_DEFLATE,
  ZIP_SIGNATURE_LEN,
} from "../lib/zip/constants.js";
import {
  FIXTURE_FILES,
  TINY_PDF,
  writeAesExtraStub,
  writeAesMethodStub,
  writeDataDescriptorNoSizeStub,
  writeLargeStoredZipFile,
  writeLocalHeaderOnly,
  writeLocalHeaderStub,
  writeMalformedExtraStub,
  writeZip,
  writeZip64SizeStub,
} from "./test-support/write-zip.js";
import { type UnzipOptions, type ZipEntry, unzipEncrypted } from "../lib/unzip.js";
import { isPdfMagic, isPdfPath } from "../lib/detect/detect.js";
import { PULL_CHUNK_SIZE } from "../lib/stream/pull.js";

/**
 * Wall-clock guard for the unknown-length (APPNOTE bit 3) scan, not a benchmark:
 * a block-wise scan of these fixtures costs milliseconds, while a per-byte
 * re-inflate of the whole accumulator costs minutes.
 *
 * It guards only the fixtures it is attached to, and those bodies are zeros —
 * a couple of KB compressed, so one or two probes cover the whole entry no
 * matter how the scan is written. Cost as a function of entry size is pinned by
 * "scales close to linearly with entry size" below, not here.
 */
const SCAN_BUDGET_MS = 5000;
/** Room for the budget assertion to be reported instead of killing the run. */
const SCAN_TEST_TIMEOUT_MS = 20_000;
/**
 * Zeros deflate about 1000:1, so this body compresses to ~32 KB: past zlib's
 * default 16 KB write buffer (`inflate.write` returns false and the pump parks
 * on `drain`) yet inside one `PULL_CHUNK_SIZE` read, so the boundary probe
 * reaches Z_STREAM_END on the first chunk. Under ~17 MB the compressed stream
 * fits the write buffer, the pump never parks, and the window below cannot be
 * hit at all.
 */
const BACKPRESSURE_BODY_SIZE = 32 * 1024 * 1024;
/**
 * Long enough for the inflate to fill the entry's buffer and stall. Destroying
 * on the same turn (the two tests above) aborts inside `pull.read` instead,
 * which is the path that already works — do not "simplify" this delay away.
 */
const BACKPRESSURE_DESTROY_DELAY_MS = 100;
/** Four timed reads; ~11 s while the scan is quadratic, ~1 s once it is not. */
const SCALING_TEST_TIMEOUT_MS = 120_000;
/** ~130 ms today: far enough above timer noise to divide by. */
const SCALING_SMALL_SIZE = 4 * 1024 * 1024;
/** 8× `SCALING_SMALL_SIZE`, so a linear reader costs ~8× the time. */
const SCALING_LARGE_SIZE = 8 * SCALING_SMALL_SIZE;
/**
 * 8× the data. The linear reference is the known-size deflate path over the same
 * bodies on the same machine: 4 MB 10.8 ms → 32 MB 99.0 ms, i.e. **9.2×**, a
 * little over 8× because per-byte cost grows with GC pressure. The probe-per-chunk
 * scan instead measures 31.6×, 33.3×, 35.8×, 37.3× (4 MB ≈ 0.13 s, 32 MB ≈ 4.6 s).
 * 16× sits between the two regimes: ~2× under the cheapest quadratic run and
 * ~1.7× over the linear reference.
 *
 * Both halves of the scan have to go for this to pass: the probe that re-inflates
 * the accumulator, and the `Buffer.concat` that regrows the whole plaintext
 * accumulator once per pull chunk (quadratic in memcpy on incompressible data).
 */
const SCALING_MAX_FACTOR = 16;
/** Zeros again: ~256 KB of deflate that the skip path inflates to 256 MB. */
const SKIP_BOMB_SIZE = 256 * 1024 * 1024;
/** Cap that must not turn into `entry exceeds maxEntrySize` for a skipped entry. */
const SKIP_BOMB_MAX_ENTRY_SIZE = 64 * 1024;
/** Chunks fed to the streaming deflate that builds a bomb fixture. */
const BOMB_FIXTURE_CHUNK_SIZE = 1024 * 1024;
/** Interval of the rss sampler; the probe blocks the loop, so keep it short. */
const RSS_SAMPLE_INTERVAL_MS = 10;
/**
 * Measured peak rss growth while skipping the bomb: 517, 518, 520 MB — two
 * 256 MB probe buffers alive at once, one per pull chunk of the compressed
 * stream. A skip that only aligns the descriptor keeps a 64 KB accumulator and
 * zlib's own buffers, so 64 MB is ~8× under today's number and far above what
 * streaming needs. rss (not `heapUsed`) because the buffer is external.
 */
const SKIP_BOMB_RSS_BUDGET_MB = 64;
/** The bomb fixture deflates 256 MB of zeros chunk by chunk before the read. */
const SKIP_BOMB_TIMEOUT_MS = 60_000;
/** BZIP2 (APPNOTE method 12): a real method this reader still has to reject. */
const ZIP_METHOD_BZIP2 = 12;

function chunked(data: Buffer, size = 3): Readable {
  const chunks: Buffer[] = [];

  for (let i = 0; i < data.length; i += size) {
    chunks.push(Buffer.from(data.subarray(i, i + size)));
  }

  return Readable.from(chunks);
}

async function collect(entry: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];

  for await (const chunk of entry) {
    chunks.push(Buffer.from(chunk));
  }

  return Buffer.concat(chunks);
}

async function collectNamed(
  source: Readable,
  password: string,
  filter?: (path: string) => boolean,
  extra?: Partial<UnzipOptions>,
): Promise<{ data: Buffer; path: string; type: ZipEntry["type"] }[]> {
  const out: { data: Buffer; path: string; type: ZipEntry["type"] }[] = [];

  for await (const entry of unzipEncrypted(source, {
    filter,
    password,
    ...extra,
  })) {
    out.push({
      data: await collect(entry),
      path: entry.path,
      type: entry.type,
    });
  }

  return out;
}

/** `--expose-gc` is not on for this suite; use `gc` only when it happens to be. */
function collectGarbage(): void {
  (globalThis as { gc?: () => void }).gc?.();
}

/**
 * Peak rss across a read. A single sample after the fact misses the allocation:
 * the boundary probe frees its buffer as soon as the entry ends.
 */
function trackPeakRss(): { stop: () => number } {
  let peak = process.memoryUsage.rss();
  const timer = setInterval(() => {
    peak = Math.max(peak, process.memoryUsage.rss());
  }, RSS_SAMPLE_INTERVAL_MS);

  return {
    stop(): number {
      clearInterval(timer);

      return Math.max(peak, process.memoryUsage.rss());
    },
  };
}

/**
 * One bit-3 deflate entry (zero local crc/sizes + 16-byte descriptor) whose
 * body is deflated chunk by chunk, so a multi-hundred-MB fixture never holds
 * its own plaintext. `writeZip` would allocate all of it, and that discarded
 * buffer would sit in rss and hide what the reader itself allocates.
 */
async function writeZeroBombEntry(name: string, size: number): Promise<Buffer> {
  const chunk = Buffer.alloc(BOMB_FIXTURE_CHUNK_SIZE, 0x00);
  const deflate = createDeflateRaw();
  const parts: Buffer[] = [];

  deflate.on("data", (piece: Buffer) => {
    parts.push(Buffer.from(piece));
  });

  let crc = 0;
  let remaining = size;

  while (remaining > 0) {
    const slice = chunk.subarray(0, Math.min(remaining, chunk.length));

    crc = crc32(slice, crc) >>> 0;
    if (!deflate.write(slice)) {
      await once(deflate, "drain");
    }
    remaining -= slice.length;
  }
  deflate.end();
  await finished(deflate);

  const payload = Buffer.concat(parts);
  const descriptor = Buffer.alloc(DATA_DESCRIPTOR_WITH_SIG_LEN);

  descriptor.writeUInt32LE(DD_SIG, 0);
  descriptor.writeUInt32LE(crc, 4);
  descriptor.writeUInt32LE(payload.length, 8);
  descriptor.writeUInt32LE(size, 12);

  return Buffer.concat([
    writeLocalHeaderOnly({
      flags: ZIP_FLAG_UTF8 | ZIP_FLAG_DATA_DESCRIPTOR,
      method: ZIP_METHOD_DEFLATE,
      name,
    }),
    payload,
    descriptor,
  ]);
}

/**
 * Wall-clock of one full unknown-length read, fixture build excluded. Bytes go
 * to a counting sink so the timing is the reader's cost, not a concat of the
 * whole entry.
 */
async function timeUnknownLengthRead(size: number): Promise<number> {
  const zip = writeZip([{ data: randomBytes(size), method: ZIP_METHOD_DEFLATE, name: "big.bin" }], {
    dataDescriptor: "16",
    omitLocalSizes: true,
  });
  let read = 0;
  const started = performance.now();

  for await (const entry of unzipEncrypted(Readable.from([zip]), { password: "" })) {
    await pipeline(
      entry,
      new Writable({
        highWaterMark: PULL_CHUNK_SIZE,
        write(piece: Buffer, _enc, cb): void {
          read += piece.length;
          cb();
        },
      }),
    );
  }
  const elapsed = performance.now() - started;

  expect(read).toBe(size);

  return elapsed;
}

/**
 * Best of two reads. Noise (JIT, a busy CI box, a GC pause) can only make a read
 * slower, so the minimum is the estimate that keeps the ratio below meaningful.
 */
async function bestUnknownLengthRead(size: number): Promise<number> {
  const first = await timeUnknownLengthRead(size);
  const second = await timeUnknownLengthRead(size);

  return Math.min(first, second);
}

describe("local header parser", () => {
  it("yields unencrypted names in order and stops at the central directory", async () => {
    const zip = writeZip(FIXTURE_FILES);
    const entries = await collectNamed(chunked(zip, 2), "unused");

    expect(entries.map((e) => e.path)).toEqual(["a.txt", "nested/b.pdf", "c.bin"]);
    expect(entries.map((e) => e.data)).toEqual([
      Buffer.from("hello\n"),
      TINY_PDF,
      Buffer.from([0, 1, 2]),
    ]);
  });

  it("throws when AES compression method is used", async () => {
    const zip = writeAesMethodStub("secret.pdf");

    await expect(collectNamed(Readable.from([zip]), "secret")).rejects.toThrow(
      /AES zip not supported: secret\.pdf/,
    );
  });

  it("throws when a WinZip AES extra field is present", async () => {
    const zip = writeAesExtraStub("secret.pdf");

    await expect(collectNamed(Readable.from([zip]), "secret")).rejects.toThrow(
      /AES zip not supported: secret\.pdf/,
    );
  });

  it("throws when a stored entry has bit3 set and local compressed size is 0", async () => {
    const zip = writeDataDescriptorNoSizeStub("open.bin");

    await expect(collectNamed(Readable.from([zip]), "secret")).rejects.toThrow(
      /no size in local header: open\.bin/,
    );
  });

  it("throws on Zip64 size sentinels", async () => {
    const zip = writeZip64SizeStub("huge.bin");

    await expect(collectNamed(Readable.from([zip]), "secret")).rejects.toThrow(
      /Zip64 not supported: huge\.bin/,
    );
  });

  it("throws on a malformed extra field instead of skipping AES extra", async () => {
    const zip = writeMalformedExtraStub("secret.pdf");

    await expect(collectNamed(Readable.from([zip]), "secret")).rejects.toThrow(
      /malformed extra field/,
    );
  });

  it("throws when the next record is neither a local header, central directory, nor EOCD", async () => {
    const body = Buffer.from("hello\n");
    // A data descriptor where the next local header belongs: the shape a
    // misaligned bit-3 scan leaves behind. The parser must not guess past it.
    const stray = Buffer.alloc(ZIP_SIGNATURE_LEN);

    stray.writeUInt32LE(DD_SIG, 0);
    const zip = Buffer.concat([
      writeLocalHeaderOnly({
        compressedSize: body.length,
        crc: crc32(body) >>> 0,
        method: 0,
        name: "a.txt",
        uncompressedSize: body.length,
      }),
      body,
      stray,
    ]);

    await expect(collectNamed(Readable.from([zip]), "")).rejects.toThrow(
      /bad zip signature 0x8074b50/,
    );
  });

  it("throws on a compression method other than stored or deflate", async () => {
    const zip = writeLocalHeaderStub({ method: ZIP_METHOD_BZIP2, name: "old.bin" });

    await expect(collectNamed(Readable.from([zip]), "")).rejects.toThrow(
      /unsupported compression method 12: old\.bin/,
    );
  });
});

describe("entry body stream", () => {
  it("autodrains unused files so the next header can be parsed", async () => {
    const zip = writeZip(FIXTURE_FILES);
    const names: string[] = [];

    for await (const entry of unzipEncrypted(chunked(zip), {
      password: "unused",
    })) {
      names.push(entry.path);
      if (entry.path !== "c.bin") {
        await entry.autodrain();
        continue;
      }
      expect(await collect(entry)).toEqual(Buffer.from([0, 1, 2]));
    }
    expect(names).toEqual(["a.txt", "nested/b.pdf", "c.bin"]);
  });

  it("does not buffer a large stored file before the sink", async () => {
    const size = 16 * 1024 * 1024;
    const dir = await mkdtemp(path.join(os.tmpdir(), "zip-rss-"));
    const file = path.join(dir, "large.zip");

    await writeLargeStoredZipFile(file, "big.bin", size);
    const fsSrc = createReadStream(file, { highWaterMark: 64 * 1024 });

    try {
      let sourceBytes = 0;
      let received = 0;
      let peakLag = 0;
      const meter = new Transform({
        highWaterMark: 64 * 1024,
        transform(chunk, _enc, cb) {
          sourceBytes += chunk.length;
          peakLag = Math.max(peakLag, sourceBytes - received);
          cb(null, chunk);
        },
      });

      fsSrc.pipe(meter);
      for await (const entry of unzipEncrypted(meter, { password: "" })) {
        await pipeline(
          entry,
          new Writable({
            highWaterMark: 64 * 1024,
            write(chunk, _enc, cb) {
              received += chunk.length;
              peakLag = Math.max(peakLag, sourceBytes - received);
              cb();
            },
          }),
        );
      }
      expect(received).toBe(size);
      expect(peakLag).toBeLessThan(1024 * 1024);
    } finally {
      fsSrc.destroy();
      await unlink(file);
    }
  });

  it("discards a directory payload so the next file still extracts", async () => {
    const dirHeader = writeLocalHeaderOnly({
      compressedSize: 4,
      method: 0,
      name: "foo/",
      uncompressedSize: 4,
    });
    const zip = Buffer.concat([
      dirHeader,
      Buffer.from("junk"),
      writeZip([{ data: Buffer.from("hello\n"), method: 0, name: "a.txt" }]),
    ]);
    const entries = await collectNamed(Readable.from([zip]), "");

    expect(entries.map((e) => e.path)).toEqual(["foo/", "a.txt"]);
    expect(entries[0]?.type).toBe("Directory");
    expect(entries[1]?.data.equals(Buffer.from("hello\n"))).toBe(true);
  });

  it("keeps the body of a file whose name ends with a backslash", async () => {
    const body = Buffer.from("data");
    const zip = Buffer.concat([
      writeLocalHeaderOnly({
        compressedSize: body.length,
        crc: crc32(body) >>> 0,
        method: 0,
        name: "weird\\",
        uncompressedSize: body.length,
      }),
      body,
    ]);
    const entries = await collectNamed(Readable.from([zip]), "");

    expect(entries[0]?.type).toBe("File");
    expect(entries[0]?.data.toString()).toBe("data");
  });

  it("treats an empty backslash-terminated entry as a directory", async () => {
    const zip = Buffer.concat([
      writeLocalHeaderOnly({
        compressedSize: 0,
        method: 0,
        name: "weird\\",
        uncompressedSize: 0,
      }),
    ]);
    const entries = await collectNamed(Readable.from([zip]), "");

    expect(entries[0]?.type).toBe("Directory");
  });

  it("continues iteration when an unread entry is destroyed", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });
    const seen: string[] = [];

    for await (const entry of unzipEncrypted(Readable.from([zip]), {
      password: "secret",
    })) {
      seen.push(entry.path);
      if (entry.path === "a.txt") {
        entry.destroy();
        continue;
      }
      await entry.autodrain();
    }
    expect(seen).toEqual(["a.txt", "nested/b.pdf", "c.bin"]);
  });

  it("continues iteration when an entry is destroyed mid-read", async () => {
    const big = Buffer.alloc(512 * 1024, 0x61);
    const zip = writeZip([
      { data: big, method: 0, name: "big.bin" },
      { data: Buffer.from("after\n"), method: 0, name: "after.txt" },
    ]);
    const seen: string[] = [];
    let tail: Buffer | undefined;

    for await (const entry of unzipEncrypted(chunked(zip, 8192), {
      password: "",
    })) {
      seen.push(entry.path);
      if (entry.path === "big.bin") {
        await once(entry, "readable");
        entry.read();
        entry.destroy();
        continue;
      }
      tail = await collect(entry);
    }
    expect(seen).toEqual(["big.bin", "after.txt"]);
    expect(tail?.equals(Buffer.from("after\n"))).toBe(true);
  });

  it("destroys the source stream when the consumer breaks early", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });
    const source = Readable.from([zip]);

    for await (const entry of unzipEncrypted(source, { password: "secret" })) {
      await entry.autodrain();
      break;
    }
    expect(source.destroyed).toBe(true);
  });

  it("does not hang the generator when deflate fails under a catching consumer", async () => {
    const payload = Buffer.alloc(32, 0x03);
    const header = writeLocalHeaderOnly({
      compressedSize: payload.length,
      method: ZIP_METHOD_DEFLATE,
      name: "bad.txt",
      uncompressedSize: 64,
    });
    const eocd = Buffer.alloc(EOCD_LEN);

    eocd.writeUInt32LE(EOCD, 0);
    const zip = Buffer.concat([header, payload, eocd]);

    let sawEntryError = false;

    await expect(
      (async () => {
        for await (const entry of unzipEncrypted(Readable.from([zip]), {
          password: "",
        })) {
          try {
            await collect(entry);
          } catch {
            sawEntryError = true;
          }
        }
      })(),
    ).rejects.toThrow();
    expect(sawEntryError).toBe(true);
  });
});

describe("optional password", () => {
  it("reads an unencrypted archive with no password option", async () => {
    const zip = writeZip(FIXTURE_FILES);
    const out: string[] = [];

    for await (const entry of unzipEncrypted(Readable.from([zip]), {})) {
      out.push(entry.path);
      await entry.autodrain();
    }
    expect(out).toEqual(["a.txt", "nested/b.pdf", "c.bin"]);
  });

  it("reads an unencrypted archive without options (one argument)", async () => {
    const zip = writeZip(FIXTURE_FILES);
    const out: string[] = [];

    for await (const entry of unzipEncrypted(Readable.from([zip]))) {
      out.push(entry.path);
      await collect(entry);
    }
    expect(out).toEqual(["a.txt", "nested/b.pdf", "c.bin"]);
  });

  it("names the entry that needs a password", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });

    await expect(
      (async () => {
        for await (const entry of unzipEncrypted(Readable.from([zip]), {})) {
          await entry.autodrain();
        }
      })(),
    ).rejects.toThrow(/password required: a\.txt/);
  });
});

describe("size limits", () => {
  it("stops a deflate entry that grows past maxEntrySize", async () => {
    const bomb = Buffer.alloc(1024 * 1024, 0x00);
    const zip = writeZip([{ data: bomb, method: ZIP_METHOD_DEFLATE, name: "bomb.bin" }]);

    await expect(
      (async () => {
        for await (const entry of unzipEncrypted(Readable.from([zip]), {
          maxEntrySize: 64 * 1024,
          password: "",
        })) {
          await collect(entry);
        }
      })(),
    ).rejects.toThrow(/entry exceeds maxEntrySize: bomb\.bin/);
  });

  it("accepts an entry exactly at maxEntrySize", async () => {
    const data = Buffer.alloc(4096, 0x41);
    const zip = writeZip([{ data, method: ZIP_METHOD_DEFLATE, name: "fits.bin" }]);
    const entries = await collectNamed(Readable.from([zip]), "", undefined, {
      maxEntrySize: data.length,
    });

    expect(entries[0]?.data.length).toBe(data.length);
  });

  it("rejects a deflate entry whose local header understates the size", async () => {
    const zip = writeZip([
      { data: Buffer.alloc(4096, 0x42), method: ZIP_METHOD_DEFLATE, name: "liar.bin" },
    ]);

    // uncompressedSize lives at local header offset 22
    zip.writeUInt32LE(16, 22);
    await expect(
      (async () => {
        for await (const entry of unzipEncrypted(Readable.from([zip]), {
          password: "",
        })) {
          await collect(entry);
        }
      })(),
    ).rejects.toThrow(/size mismatch: liar\.bin/);
  });

  it("rejects a stored entry whose local header overstates the size", async () => {
    const zip = writeZip([{ data: Buffer.from("abc"), method: 0, name: "short.bin" }]);

    // Lie about uncompressed size (not AES method 99).
    const overstatedUncompressedSize = 99;

    zip.writeUInt32LE(overstatedUncompressedSize, 22);
    await expect(
      (async () => {
        for await (const entry of unzipEncrypted(Readable.from([zip]), {
          password: "",
        })) {
          await collect(entry);
        }
      })(),
    ).rejects.toThrow(/size mismatch: short\.bin/);
  });
});

describe("password path", () => {
  it("extracts every ZipCrypto file including the second (keys reset per entry)", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });
    const entries = await collectNamed(chunked(zip, 1), "secret");

    expect(entries.map((e) => e.path)).toEqual(["a.txt", "nested/b.pdf", "c.bin"]);
    expect(entries[0]?.data.equals(Buffer.from("hello\n"))).toBe(true);
    expect(entries[1]?.data.equals(TINY_PDF)).toBe(true);
    expect(entries[2]?.data.equals(Buffer.from([0, 1, 2]))).toBe(true);
  });

  it("rejects a wrong password without emitting garbage", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });

    await expect(collectNamed(Readable.from([zip]), "wrong")).rejects.toThrow(
      /invalid zip password/,
    );
  });

  it("skips filter misses without yielding them", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });
    const entries = await collectNamed(chunked(zip), "secret", (name) => name.endsWith(".pdf"));

    expect(entries.map((e) => e.path)).toEqual(["nested/b.pdf"]);
    expect(entries[0]?.data.equals(TINY_PDF)).toBe(true);
  });

  it("still fails the password on an encrypted filter skip", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });

    await expect(collectNamed(Readable.from([zip]), "nope", () => false)).rejects.toThrow(
      /invalid zip password/,
    );
  });

  it("pipelines the middle PDF to equal fixture bytes", async () => {
    const zip = writeZip(FIXTURE_FILES, { password: "secret" });
    const chunks: Buffer[] = [];

    for await (const entry of unzipEncrypted(chunked(zip, 5), {
      password: "secret",
    })) {
      if (!isPdfPath(entry.path)) {
        await entry.autodrain();
        continue;
      }
      await pipeline(
        entry,
        new Writable({
          write(chunk, _enc, cb) {
            const buf = Buffer.from(chunk);

            if (chunks.length === 0) {
              expect(isPdfMagic(buf)).toBe(true);
            }
            chunks.push(buf);
            cb();
          },
        }),
      );
    }
    expect(Buffer.concat(chunks).equals(TINY_PDF)).toBe(true);
  });

  it("skips a 16-byte data descriptor after a sized encrypted body", async () => {
    const zip = writeZip([{ data: Buffer.from("hello\n"), method: 0, name: "a.txt" }], {
      dataDescriptor: "16",
      password: "secret",
    });
    const entries = await collectNamed(Readable.from([zip]), "secret");

    expect(entries).toHaveLength(1);
    expect(entries[0]?.data.equals(Buffer.from("hello\n"))).toBe(true);
  });

  it("skips a 12-byte data descriptor after a sized stored body", async () => {
    const zip = writeZip(
      [
        { data: Buffer.from("hello\n"), method: 0, name: "a.txt" },
        { data: Buffer.from("world\n"), method: 0, name: "b.txt" },
      ],
      { dataDescriptor: "12" },
    );
    const entries = await collectNamed(Readable.from([zip]), "");

    expect(entries.map((e) => e.data.toString())).toEqual(["hello\n", "world\n"]);
  });

  it("validates the data descriptor when the local header carries no crc", async () => {
    const zip = writeZip(
      [{ data: Buffer.from("hello\n"), method: ZIP_METHOD_DEFLATE, name: "a.txt" }],
      {
        dataDescriptor: "16",
        zeroLocalCrc: true,
      },
    );
    const entries = await collectNamed(Readable.from([zip]), "");

    expect(entries[0]?.data.toString()).toBe("hello\n");
  });

  it.each([
    ["crc", /crc mismatch: a\.txt/],
    ["uncompressedSize", /size mismatch: a\.txt/],
    ["compressedSize", /size mismatch: a\.txt/],
  ] as const)("rejects a data descriptor with a wrong %s", async (field, message) => {
    const zip = writeZip(
      [{ data: Buffer.from("hello\n"), method: ZIP_METHOD_DEFLATE, name: "a.txt" }],
      {
        corruptDescriptor: field,
        dataDescriptor: "16",
        zeroLocalCrc: true,
      },
    );

    await expect(collectNamed(Readable.from([zip]), "")).rejects.toThrow(message);
  });

  it("decodes a latin1 name when the UTF-8 flag is unset", async () => {
    const zip = writeZip([{ data: Buffer.from("ok"), method: 0, name: "café.txt" }], {
      utf8: false,
    });
    const entries = await collectNamed(Readable.from([zip]), "");

    expect(entries[0]?.path).toBe("café.txt");
  });

  it("decrypts with a latin1 password encoding", async () => {
    const password = "café";
    const zip = writeZip([{ data: Buffer.from("hello\n"), method: 0, name: "a.txt" }], {
      password,
      passwordEncoding: "latin1",
    });
    const entries = await collectNamed(Readable.from([zip]), password, undefined, {
      passwordEncoding: "latin1",
    });

    expect(entries[0]?.data.equals(Buffer.from("hello\n"))).toBe(true);
  });
});

describe("unknown-length data descriptor", () => {
  it("extracts an unencrypted deflate file with bit3 and omitted local sizes (16-byte descriptor)", async () => {
    const zip = writeZip(
      [{ data: Buffer.from("hello\n"), method: ZIP_METHOD_DEFLATE, name: "a.txt" }],
      { dataDescriptor: "16", omitLocalSizes: true },
    );
    const entries = await collectNamed(chunked(zip), "");

    expect(entries).toHaveLength(1);
    expect(entries[0]?.path).toBe("a.txt");
    expect(entries[0]?.data.equals(Buffer.from("hello\n"))).toBe(true);
  });

  it("extracts an unencrypted deflate file with bit3 and omitted local sizes (12-byte descriptor)", async () => {
    const zip = writeZip(
      [{ data: Buffer.from("hello\n"), method: ZIP_METHOD_DEFLATE, name: "a.txt" }],
      { dataDescriptor: "12", omitLocalSizes: true },
    );
    const entries = await collectNamed(chunked(zip), "");

    expect(entries).toHaveLength(1);
    expect(entries[0]?.path).toBe("a.txt");
    expect(entries[0]?.data.equals(Buffer.from("hello\n"))).toBe(true);
  });

  it("extracts two unencrypted deflate files with omitted local sizes", async () => {
    const zip = writeZip(
      [
        { data: Buffer.from("hello\n"), method: ZIP_METHOD_DEFLATE, name: "a.txt" },
        { data: Buffer.from("world\n"), method: ZIP_METHOD_DEFLATE, name: "b.txt" },
      ],
      { dataDescriptor: "16", omitLocalSizes: true },
    );
    const entries = await collectNamed(chunked(zip), "");

    expect(entries.map((e) => e.path)).toEqual(["a.txt", "b.txt"]);
    expect(entries.map((e) => e.data.toString())).toEqual(["hello\n", "world\n"]);
  });

  it("extracts an encrypted deflate file with omitted local sizes", async () => {
    const zip = writeZip(
      [{ data: Buffer.from("hello\n"), method: ZIP_METHOD_DEFLATE, name: "a.txt" }],
      {
        dataDescriptor: "16",
        omitLocalSizes: true,
        password: "secret",
      },
    );
    const entries = await collectNamed(chunked(zip), "secret");

    expect(entries).toHaveLength(1);
    expect(entries[0]?.path).toBe("a.txt");
    expect(entries[0]?.data.equals(Buffer.from("hello\n"))).toBe(true);
  });

  it("emits more than one data chunk for an unknown-length deflate file larger than PULL_CHUNK_SIZE", async () => {
    const payload = randomBytes(PULL_CHUNK_SIZE + 1);
    const zip = writeZip([{ data: payload, method: ZIP_METHOD_DEFLATE, name: "big.bin" }], {
      dataDescriptor: "16",
      omitLocalSizes: true,
    });
    let chunkCount = 0;
    const chunks: Buffer[] = [];

    for await (const entry of unzipEncrypted(chunked(zip, 8192), { password: "" })) {
      expect(entry.path).toBe("big.bin");
      // Concatenated length still passes if inflateRawSync pushes once; count `data` events.
      entry.on("data", (chunk: Buffer) => {
        chunkCount += 1;
        chunks.push(Buffer.from(chunk));
      });
      await once(entry, "end");
    }

    expect(Buffer.concat(chunks).equals(payload)).toBe(true);
    expect(chunkCount).toBeGreaterThan(1);
  });

  it("extracts an encrypted unknown-length deflate file larger than PULL_CHUNK_SIZE", async () => {
    const payload = randomBytes(PULL_CHUNK_SIZE + 1);
    const zip = writeZip([{ data: payload, method: ZIP_METHOD_DEFLATE, name: "big.bin" }], {
      dataDescriptor: "16",
      omitLocalSizes: true,
      password: "secret",
    });
    let chunkCount = 0;
    const chunks: Buffer[] = [];

    for await (const entry of unzipEncrypted(chunked(zip, 8192), { password: "secret" })) {
      expect(entry.path).toBe("big.bin");
      entry.on("data", (chunk: Buffer) => {
        chunkCount += 1;
        chunks.push(Buffer.from(chunk));
      });
      await once(entry, "end");
    }

    expect(Buffer.concat(chunks).equals(payload)).toBe(true);
    expect(chunkCount).toBeGreaterThan(1);
  });

  it("rejects a wrong password on an encrypted unknown-length entry", async () => {
    const zip = writeZip(
      [{ data: Buffer.from("hello\n"), method: ZIP_METHOD_DEFLATE, name: "a.txt" }],
      {
        dataDescriptor: "16",
        omitLocalSizes: true,
        password: "secret",
      },
    );

    await expect(collectNamed(Readable.from([zip]), "wrong")).rejects.toThrow(
      /invalid zip password/,
    );
  });

  it("skips a filtered unknown-length encrypted entry and yields the PDF", async () => {
    const zip = writeZip(
      [
        { data: Buffer.from("hello\n"), method: ZIP_METHOD_DEFLATE, name: "a.txt" },
        { data: TINY_PDF, method: ZIP_METHOD_DEFLATE, name: "nested/b.pdf" },
      ],
      {
        dataDescriptor: "16",
        omitLocalSizes: true,
        password: "secret",
      },
    );
    const entries = await collectNamed(chunked(zip), "secret", (name) => name.endsWith(".pdf"));

    expect(entries.map((e) => e.path)).toEqual(["nested/b.pdf"]);
    expect(entries[0]?.data.equals(TINY_PDF)).toBe(true);
  });

  it("extracts the next file after skipping an unknown-length encrypted entry", async () => {
    const zip = writeZip(
      [
        { data: Buffer.from("hello\n"), method: ZIP_METHOD_DEFLATE, name: "a.txt" },
        { data: TINY_PDF, method: ZIP_METHOD_DEFLATE, name: "nested/b.pdf" },
      ],
      {
        dataDescriptor: "16",
        omitLocalSizes: true,
        password: "secret",
      },
    );
    const seen: { data: Buffer; path: string }[] = [];

    for await (const entry of unzipEncrypted(chunked(zip), { password: "secret" })) {
      if (entry.path === "a.txt") {
        await entry.autodrain();
        continue;
      }
      seen.push({ data: await collect(entry), path: entry.path });
    }

    expect(seen.map((e) => e.path)).toEqual(["nested/b.pdf"]);
    expect(seen[0]?.data.equals(TINY_PDF)).toBe(true);
  });

  it("throws when a yielded unknown-length entry exceeds maxEntrySize", async () => {
    // 64 KB of zeros deflates to a few dozen bytes, so this stays cheap to scan.
    const bomb = Buffer.alloc(64 * 1024, 0x00);
    const zip = writeZip([{ data: bomb, method: ZIP_METHOD_DEFLATE, name: "bomb.bin" }], {
      dataDescriptor: "16",
      omitLocalSizes: true,
    });

    await expect(
      collectNamed(chunked(zip, 8192), "", undefined, { maxEntrySize: 4096 }),
    ).rejects.toThrow(/entry exceeds maxEntrySize: bomb\.bin/);
  });

  it(
    "fails an unknown-length zip bomb before materialising it",
    { timeout: SCAN_TEST_TIMEOUT_MS },
    async () => {
      // 8 MB of zeros deflates to a few KB: the cap has to bite during inflation,
      // otherwise the whole 8 MB is allocated (and re-inflated) before it is seen.
      const bomb = Buffer.alloc(8 * 1024 * 1024, 0x00);
      const zip = writeZip([{ data: bomb, method: ZIP_METHOD_DEFLATE, name: "bomb.bin" }], {
        dataDescriptor: "16",
        omitLocalSizes: true,
      });
      const started = performance.now();

      await expect(
        collectNamed(chunked(zip, 8192), "", undefined, { maxEntrySize: 64 * 1024 }),
      ).rejects.toThrow(/entry exceeds maxEntrySize: bomb\.bin/);

      expect(performance.now() - started).toBeLessThan(SCAN_BUDGET_MS);
    },
  );

  it("does not apply maxEntrySize to a filtered unknown-length entry", async () => {
    // 1 MB of zeros is far past the cap while still deflating to ~1 KB, so today's
    // per-byte scan reaches the wrong throw quickly.
    const zip = writeZip(
      [
        { data: Buffer.alloc(1024 * 1024, 0x00), method: ZIP_METHOD_DEFLATE, name: "big.bin" },
        { data: TINY_PDF, method: ZIP_METHOD_DEFLATE, name: "nested/b.pdf" },
      ],
      { dataDescriptor: "16", omitLocalSizes: true },
    );
    const entries = await collectNamed(chunked(zip, 8192), "", (name) => name.endsWith(".pdf"), {
      maxEntrySize: 64 * 1024,
    });

    expect(entries.map((e) => e.path)).toEqual(["nested/b.pdf"]);
    expect(entries[0]?.data.equals(TINY_PDF)).toBe(true);
  });

  // Keep this before the scaling test: rss growth is measured against a baseline,
  // and a test that has just allocated hundreds of MB leaves freed pages the next
  // allocation reuses, which shrinks the delta (518 MB alone, 314 MB after it).
  it(
    "does not allocate the plaintext of a filtered unknown-length bomb",
    { timeout: SKIP_BOMB_TIMEOUT_MS },
    async () => {
      // Skipping is allowed to inflate (the descriptor has to be found) but not to
      // keep the output: the boundary probe returns the entry's whole plaintext,
      // and on this path it is called with no `maxOutputLength` at all, so a
      // filtered bomb is materialised even under a small `maxEntrySize`.
      const zip = Buffer.concat([
        await writeZeroBombEntry("bomb.bin", SKIP_BOMB_SIZE),
        writeZip(
          [{ data: Buffer.from("after\n"), method: ZIP_METHOD_DEFLATE, name: "after.txt" }],
          { dataDescriptor: "16", omitLocalSizes: true },
        ),
      ]);

      collectGarbage();
      const baseline = process.memoryUsage.rss();
      const rss = trackPeakRss();
      let entries: { data: Buffer; path: string; type: ZipEntry["type"] }[] = [];
      let peakRssGrowthMb = 0;

      try {
        entries = await collectNamed(chunked(zip, PULL_CHUNK_SIZE), "", (n) => n === "after.txt", {
          maxEntrySize: SKIP_BOMB_MAX_ENTRY_SIZE,
        });
      } finally {
        peakRssGrowthMb = Math.round((rss.stop() - baseline) / (1024 * 1024));
      }

      // The skip must stay silent about the cap (public contract), and cheap.
      expect(entries.map((e) => e.path)).toEqual(["after.txt"]);
      expect(entries[0]?.data.equals(Buffer.from("after\n"))).toBe(true);
      expect(peakRssGrowthMb).toBeLessThan(SKIP_BOMB_RSS_BUDGET_MB);
    },
  );

  it(
    "scales close to linearly with entry size on the unknown-length path",
    { timeout: SCALING_TEST_TIMEOUT_MS },
    async () => {
      // A ratio, not a millisecond budget: a fixed budget on a body this size is
      // CI-flaky, while re-inflating the whole accumulator once per pull chunk is
      // quadratic and shows up as a factor no amount of CI noise explains.
      const small = await bestUnknownLengthRead(SCALING_SMALL_SIZE);
      const large = await bestUnknownLengthRead(SCALING_LARGE_SIZE);

      expect(large / small).toBeLessThan(SCALING_MAX_FACTOR);
    },
  );

  it(
    "continues iteration when a large unknown-length entry is destroyed unread",
    { timeout: SCAN_TEST_TIMEOUT_MS },
    async () => {
      // Unread destroy() still inflates internally to the data descriptor (APPNOTE
      // bit 3) so the next local header stays aligned — it is not a cheap skip.
      const big = randomBytes(256 * 1024);
      const zip = writeZip(
        [
          { data: big, method: ZIP_METHOD_DEFLATE, name: "big.bin" },
          { data: Buffer.from("after\n"), method: ZIP_METHOD_DEFLATE, name: "after.txt" },
        ],
        { dataDescriptor: "16", omitLocalSizes: true },
      );
      const seen: string[] = [];
      const started = performance.now();
      let tail: Buffer | undefined;

      for await (const entry of unzipEncrypted(chunked(zip, 8192), { password: "" })) {
        seen.push(entry.path);
        if (entry.path === "big.bin") {
          entry.destroy();
          continue;
        }
        tail = await collect(entry);
      }

      expect(seen).toEqual(["big.bin", "after.txt"]);
      expect(tail?.equals(Buffer.from("after\n"))).toBe(true);
      expect(performance.now() - started).toBeLessThan(SCAN_BUDGET_MS);
    },
  );

  it(
    "continues iteration when an unknown-length entry is destroyed mid-read",
    { timeout: SCAN_TEST_TIMEOUT_MS },
    async () => {
      const big = randomBytes(256 * 1024);
      const zip = writeZip(
        [
          { data: big, method: ZIP_METHOD_DEFLATE, name: "big.bin" },
          { data: Buffer.from("after\n"), method: ZIP_METHOD_DEFLATE, name: "after.txt" },
        ],
        { dataDescriptor: "16", omitLocalSizes: true, password: "secret" },
      );
      const seen: string[] = [];
      let tail: Buffer | undefined;

      for await (const entry of unzipEncrypted(chunked(zip, 8192), { password: "secret" })) {
        seen.push(entry.path);
        if (entry.path === "big.bin") {
          // Start consume then destroy on this turn (same-tick abort).
          entry.read();
          entry.destroy();
          continue;
        }
        tail = await collect(entry);
      }

      expect(seen).toEqual(["big.bin", "after.txt"]);
      expect(tail?.equals(Buffer.from("after\n"))).toBe(true);
    },
  );

  it(
    "continues iteration when an unknown-length entry is destroyed while the inflate is backpressured",
    { timeout: SCAN_TEST_TIMEOUT_MS },
    async () => {
      // Same-tick destroy (tests above) never parks inflate on backpressure.
      // Waiting lets zlib fill the entry buffer so `write` defers; `destroy()`
      // must still unread overshoot and yield `after.txt`, not drop the rest
      // of the archive with no error.
      const zip = writeZip(
        [
          {
            data: Buffer.alloc(BACKPRESSURE_BODY_SIZE, 0x00),
            method: ZIP_METHOD_DEFLATE,
            name: "big.bin",
          },
          { data: Buffer.from("after\n"), method: ZIP_METHOD_DEFLATE, name: "after.txt" },
        ],
        { dataDescriptor: "16", omitLocalSizes: true },
      );
      const seen: string[] = [];
      let tail: Buffer | undefined;

      for await (const entry of unzipEncrypted(Readable.from([zip]), { password: "" })) {
        seen.push(entry.path);
        if (entry.path === "big.bin") {
          entry.read();
          await sleep(BACKPRESSURE_DESTROY_DELAY_MS);
          entry.destroy();
          continue;
        }
        tail = await collect(entry);
      }

      expect(seen).toEqual(["big.bin", "after.txt"]);
      expect(tail?.equals(Buffer.from("after\n"))).toBe(true);
    },
  );

  it("extracts a directory then a file when the directory is unknown-length deflate", async () => {
    const zip = writeZip(
      [
        { data: Buffer.alloc(0), directory: true, method: ZIP_METHOD_DEFLATE, name: "dir/" },
        { data: Buffer.from("hello"), name: "a.txt" },
      ],
      { dataDescriptor: "16", omitLocalSizes: true },
    );
    const seen: {
      data: Buffer;
      directory: boolean;
      path: string;
      type: ZipEntry["type"];
    }[] = [];

    for await (const entry of unzipEncrypted(Readable.from([zip]), { password: "" })) {
      seen.push({
        data: await collect(entry),
        directory: entry.isDirectory(),
        path: entry.path,
        type: entry.type,
      });
    }

    expect(seen).toHaveLength(2);
    expect(seen[0]?.path).toBe("dir/");
    expect(seen[0]?.type).toBe("Directory");
    expect(seen[0]?.directory).toBe(true);
    expect(seen[1]?.path).toBe("a.txt");
    expect(seen[1]?.type).toBe("File");
    expect(seen[1]?.data.equals(Buffer.from("hello"))).toBe(true);
  });

  it("still yields the next file after skipping an unknown-length deflate directory", async () => {
    const zip = writeZip(
      [
        { data: Buffer.alloc(0), directory: true, method: ZIP_METHOD_DEFLATE, name: "dir/" },
        { data: Buffer.from("hello"), name: "a.txt" },
      ],
      { dataDescriptor: "16", omitLocalSizes: true },
    );
    const entries = await collectNamed(Readable.from([zip]), "", (p) => p !== "dir/");

    expect(entries.map((e) => e.path)).toEqual(["a.txt"]);
    expect(entries[0]?.data.equals(Buffer.from("hello"))).toBe(true);
  });

  it("yields a stored bit3 directory instead of demanding a local size", async () => {
    // `unknownLength` already excludes directories — an empty method-8 directory
    // has no deflate payload to scan — but the `no size in local header` throw
    // does not, so a stored directory with bit 3 and zero sizes is rejected even
    // though it has no body at all and the next 16 bytes are its descriptor.
    const zip = writeZip(
      [
        { data: Buffer.alloc(0), directory: true, method: 0, name: "dir/" },
        { data: Buffer.from("hello"), name: "a.txt" },
      ],
      { dataDescriptor: "16", omitLocalSizes: true },
    );
    const entries = await collectNamed(Readable.from([zip]), "");

    expect(entries.map((e) => e.path)).toEqual(["dir/", "a.txt"]);
    expect(entries[0]?.type).toBe("Directory");
    expect(entries[0]?.data.length).toBe(0);
    expect(entries[1]?.data.equals(Buffer.from("hello"))).toBe(true);
  });

  it("extracts a yielded unknown-length directory then a file from chunked input", async () => {
    const zip = writeZip(
      [
        { data: Buffer.alloc(0), directory: true, method: ZIP_METHOD_DEFLATE, name: "dir/" },
        { data: Buffer.from("hello"), name: "a.txt" },
      ],
      { dataDescriptor: "16", omitLocalSizes: true },
    );
    const entries = await collectNamed(chunked(zip), "");

    expect(entries.map((e) => e.path)).toEqual(["dir/", "a.txt"]);
    expect(entries[0]?.type).toBe("Directory");
    expect(entries[1]?.data.equals(Buffer.from("hello"))).toBe(true);
  });
});

describe("ZipEntry helpers", () => {
  it("reports a file entry as isFile() and not isDirectory()", async () => {
    const zip = writeZip(FIXTURE_FILES);
    const flags: { directory: boolean; file: boolean; path: string }[] = [];

    for await (const entry of unzipEncrypted(chunked(zip), { password: "" })) {
      flags.push({
        directory: entry.isDirectory(),
        file: entry.isFile(),
        path: entry.path,
      });
      await collect(entry);
    }

    expect(flags).toEqual([
      { directory: false, file: true, path: "a.txt" },
      { directory: false, file: true, path: "nested/b.pdf" },
      { directory: false, file: true, path: "c.bin" },
    ]);
  });

  it("reports a directory entry as isDirectory() and not isFile()", async () => {
    const dirHeader = writeLocalHeaderOnly({
      compressedSize: 4,
      method: 0,
      name: "foo/",
      uncompressedSize: 4,
    });
    const zip = Buffer.concat([
      dirHeader,
      Buffer.from("junk"),
      writeZip([{ data: Buffer.from("hello\n"), method: 0, name: "a.txt" }]),
    ]);
    const flags: { directory: boolean; file: boolean; path: string }[] = [];

    for await (const entry of unzipEncrypted(Readable.from([zip]), { password: "" })) {
      flags.push({
        directory: entry.isDirectory(),
        file: entry.isFile(),
        path: entry.path,
      });
      await collect(entry);
    }

    expect(flags).toEqual([
      { directory: true, file: false, path: "foo/" },
      { directory: false, file: true, path: "a.txt" },
    ]);
  });

  it("safeName() returns the basename of a backslash path", async () => {
    const zip = writeZip([{ data: Buffer.from("pdf"), method: 0, name: "a\\b\\c.pdf" }]);
    let safe: string | undefined;

    for await (const entry of unzipEncrypted(Readable.from([zip]), { password: "" })) {
      safe = entry.safeName();
      await collect(entry);
    }

    expect(safe).toBe("c.pdf");
  });

  it("safeName() returns the basename after parent-directory segments", async () => {
    const zip = writeZip([{ data: Buffer.from("x"), method: 0, name: "foo/../etc/passwd" }]);
    let safe: string | undefined;

    for await (const entry of unzipEncrypted(Readable.from([zip]), { password: "" })) {
      safe = entry.safeName();
      await collect(entry);
    }

    expect(safe).toBe("passwd");
  });

  it.each(["..", ".", "", "foo/.."] as const)(
    "safeName() throws for an unsafe name %j",
    async (name) => {
      const zip = writeZip([{ data: Buffer.from("x"), method: 0, name }]);

      await expect(
        (async () => {
          for await (const entry of unzipEncrypted(Readable.from([zip]), { password: "" })) {
            try {
              entry.safeName();
            } finally {
              await collect(entry);
            }
          }
        })(),
      ).rejects.toThrow(/unsafe entry name/);
    },
  );
});
