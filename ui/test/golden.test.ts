/**
 * Every golden spec produced by the Python chart compiler (tests/golden/charts) must
 * compile with the real vega-lite library without errors or warnings, once the view
 * has inlined its rows exactly as it does at runtime.
 */
import { readdirSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

import { compile } from "vega-lite";
import { describe, expect, it } from "vitest";

import { embeddableSpec, parsePayload, toRows, type Row } from "../src/payload";

const GOLDEN_DIR = resolve(__dirname, "../../tests/golden/charts");
const files = readdirSync(GOLDEN_DIR).filter((name) => name.endsWith(".json"));

function collectingLogger(sink: string[]) {
  const logger = {
    level: (_?: number) => logger as never,
    error: (...args: unknown[]) => (sink.push(`error: ${args.join(" ")}`), logger),
    warn: (...args: unknown[]) => (sink.push(`warn: ${args.join(" ")}`), logger),
    info: () => logger,
    debug: () => logger,
  };
  return logger;
}

describe("golden specs compile with vega-lite", () => {
  it("has golden files", () => {
    expect(files.length).toBeGreaterThan(10);
  });

  for (const file of files) {
    it(file, () => {
      const golden = JSON.parse(readFileSync(resolve(GOLDEN_DIR, file), "utf-8")) as {
        spec: Record<string, unknown>;
        data: Row[];
      };
      const messages: string[] = [];
      const spec = embeddableSpec(golden.spec, golden.data);
      const result = compile(spec as never, { logger: collectingLogger(messages) as never });
      // vega-lite always emits this for step-sized (horizontal bar) heights, whatever the
      // autosize type; the chart still fits its width. Every other message fails the test.
      const unexpected = messages.filter((m) => !m.includes('Dropping "fit-y"'));
      expect(unexpected).toEqual([]);
      expect(result.spec.data).toBeDefined();
      expect(JSON.stringify(result.spec)).not.toContain('"url"');
    });
  }
});

describe("payload contract", () => {
  const valid = {
    handle: "q_1",
    source: "shop",
    sql: "SELECT 1",
    display_sql: "SELECT\n  1",
    executed_at: "2026-11-18T00:00:00+00:00",
    elapsed_ms: 3,
    row_count: 2,
    truncated: false,
    chart_rows: 2,
    columns: [
      { name: "status", db_type: "VARCHAR", kind: "text" },
      { name: "n", db_type: "BIGINT", kind: "number" },
    ],
    data: { status: ["paid", "refunded"], n: [30, 10] },
    spec: { mark: "bar", usermeta: { embedOptions: { ast: false } }, datasets: { evil: [] } },
    summary: "2 rows",
    notes: ["note", 3],
  };

  it("parses a valid payload and drops bad notes", () => {
    const parsed = parsePayload(valid);
    expect(parsed?.handle).toBe("q_1");
    expect(parsed?.notes).toEqual(["note"]);
  });

  it("rejects non-payloads", () => {
    expect(parsePayload(null)).toBeNull();
    expect(parsePayload({ ...valid, sql: 1 })).toBeNull();
    expect(parsePayload({ ...valid, data: [] })).toBeNull();
  });

  it("turns columns into rows", () => {
    expect(toRows(valid.columns as never, valid.data)).toEqual([
      { status: "paid", n: 30 },
      { status: "refunded", n: 10 },
    ]);
  });

  it("strips usermeta and foreign datasets when embedding", () => {
    const spec = embeddableSpec(valid.spec, [{ n: 1 }]);
    expect(spec.usermeta).toBeUndefined();
    expect(spec.datasets).toEqual({ table: [{ n: 1 }] });
    expect(spec.data).toEqual({ name: "table" });
  });
});
