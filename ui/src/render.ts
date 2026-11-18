import embed, { type Result, type VisualizationSpec } from "vega-embed";

import {
  type ChartPayload,
  type ColumnKind,
  type JsonValue,
  type Row,
  embeddableSpec,
  toRows,
} from "./payload";

export type Theme = "light" | "dark";

const TABLE_ROW_LIMIT = 200;

/** A Vega loader that refuses every request: charts only use the inlined rows. */
const offlineLoader = {
  load: () => Promise.reject(new Error("External data is disabled in this view.")),
  sanitize: () => Promise.reject(new Error("External data is disabled in this view.")),
  http: () => Promise.reject(new Error("External data is disabled in this view.")),
  file: () => Promise.reject(new Error("External data is disabled in this view.")),
};

export async function renderChart(
  container: HTMLElement,
  payload: ChartPayload,
  theme: Theme,
): Promise<Result> {
  const rows = toRows(payload.columns, payload.data);
  // The view's header already shows the title; drawing it again inside the chart repeats it.
  const { title: _title, ...untitled } = payload.spec;
  const spec = embeddableSpec(untitled, rows) as VisualizationSpec;
  return embed(container, spec, {
    ast: true, // CSP-safe expression interpreter: the iframe forbids eval
    // vega-embed's loader option takes a Loader; ours rejects every request.
    loader: offlineLoader as never,
    renderer: "svg",
    theme: theme === "dark" ? "dark" : undefined,
    actions: { export: true, source: false, compiled: false, editor: false },
    config: { background: "transparent" },
  });
}

const MIDNIGHT = /^(\d{4}-\d{2}-\d{2})T00:00:00(?:\.0+)?(?:Z|[+-]00:00)?$/;

export function formatCell(value: JsonValue, kind: ColumnKind): string {
  if (value === null) return "";
  if (typeof value === "number") return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  if (typeof value === "string") {
    // Dates truncated to a day arrive as midnight timestamps; show just the date.
    return kind === "temporal" ? value.replace(MIDNIGHT, "$1") : value;
  }
  return JSON.stringify(value);
}

export function renderTable(container: HTMLElement, payload: ChartPayload): void {
  const rows: Row[] = toRows(payload.columns, payload.data).slice(0, TABLE_ROW_LIMIT);
  const table = document.createElement("table");
  const head = table.createTHead().insertRow();
  for (const column of payload.columns) {
    const th = document.createElement("th");
    th.textContent = column.name;
    th.title = column.db_type;
    if (column.kind === "number") th.className = "num";
    head.appendChild(th);
  }
  const body = table.createTBody();
  for (const row of rows) {
    const tr = body.insertRow();
    for (const column of payload.columns) {
      const td = tr.insertCell();
      td.textContent = formatCell(row[column.name] ?? null, column.kind); // never HTML
      if (column.kind === "number") td.className = "num";
    }
  }
  container.replaceChildren(table);
}
