/**
 * The data contract between the `visualize` tool and this view.
 * Mirrors `ChartPayload` in src/bi_chart_mcp/tools/chart_tools.py.
 */

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export type ColumnKind = "number" | "temporal" | "text" | "boolean" | "other";

export interface ColumnMeta {
  name: string;
  db_type: string;
  kind: ColumnKind;
}

export interface ChartPayload {
  handle: string;
  source: string;
  sql: string;
  display_sql: string;
  executed_at: string;
  elapsed_ms: number;
  row_count: number;
  truncated: boolean;
  chart_rows: number;
  columns: ColumnMeta[];
  /** Column-oriented values: `data[column][rowIndex]`. */
  data: Record<string, JsonValue[]>;
  spec: Record<string, unknown>;
  summary: string;
  notes: string[];
}

export type Row = Record<string, JsonValue>;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

const REQUIRED_STRINGS = ["handle", "source", "sql", "display_sql", "executed_at", "summary"] as const;

/** Validate untrusted tool output; returns null when it is not a chart payload. */
export function parsePayload(value: unknown): ChartPayload | null {
  if (!isRecord(value)) return null;
  for (const key of REQUIRED_STRINGS) {
    if (typeof value[key] !== "string") return null;
  }
  if (!Array.isArray(value.columns) || !isRecord(value.data) || !isRecord(value.spec)) {
    return null;
  }
  const columns = value.columns.filter(
    (c): c is ColumnMeta => isRecord(c) && typeof c.name === "string",
  );
  return {
    handle: value.handle as string,
    source: value.source as string,
    sql: value.sql as string,
    display_sql: value.display_sql as string,
    executed_at: value.executed_at as string,
    elapsed_ms: typeof value.elapsed_ms === "number" ? value.elapsed_ms : 0,
    row_count: typeof value.row_count === "number" ? value.row_count : 0,
    truncated: value.truncated === true,
    chart_rows: typeof value.chart_rows === "number" ? value.chart_rows : 0,
    columns,
    data: value.data as Record<string, JsonValue[]>,
    spec: value.spec,
    summary: value.summary as string,
    notes: Array.isArray(value.notes) ? value.notes.filter((n): n is string => typeof n === "string") : [],
  };
}

/** Column-oriented data to row objects, as Vega expects. */
export function toRows(columns: ColumnMeta[], data: Record<string, JsonValue[]>): Row[] {
  const length = Math.max(0, ...columns.map((c) => data[c.name]?.length ?? 0));
  const rows: Row[] = [];
  for (let i = 0; i < length; i += 1) {
    const row: Row = {};
    for (const column of columns) {
      row[column.name] = data[column.name]?.[i] ?? null;
    }
    rows.push(row);
  }
  return rows;
}

/**
 * Prepare the server's spec for embedding: inline the rows as the named dataset and drop
 * anything that could load external content or override embed options.
 */
export function embeddableSpec(spec: Record<string, unknown>, rows: Row[]): Record<string, unknown> {
  const { usermeta: _usermeta, datasets: _datasets, ...rest } = spec;
  return { ...rest, data: { name: "table" }, datasets: { table: rows } };
}
