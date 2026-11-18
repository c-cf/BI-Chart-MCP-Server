import {
  App,
  applyDocumentTheme,
  applyHostFonts,
  applyHostStyleVariables,
  type McpUiHostContext,
} from "@modelcontextprotocol/ext-apps/app-with-deps";
import type { Result } from "vega-embed";

import { type ChartPayload, parsePayload } from "./payload";
import { renderChart, renderTable, type Theme } from "./render";
import "./style.css";

type View = "chart" | "table";

function element<T extends HTMLElement>(id: string): T {
  const found = document.getElementById(id);
  if (!found) throw new Error(`Missing element #${id}`);
  return found as T;
}

const ui = {
  summary: element("summary"),
  meta: element("meta"),
  chart: element("chart"),
  table: element("table"),
  error: element("error"),
  notes: element<HTMLUListElement>("notes"),
  sql: element("sql"),
  showChart: element<HTMLButtonElement>("show-chart"),
  showTable: element<HTMLButtonElement>("show-table"),
  fullscreen: element<HTMLButtonElement>("fullscreen"),
};

let payload: ChartPayload | null = null;
let theme: Theme = "light";
let view: View = "chart";
let embedded: Result | null = null;
let fullscreen = false;

const app = new App({ name: "bi-chart-mcp", version: "0.2.0" }, {}, { autoResize: true });

function showError(message: string): void {
  ui.error.textContent = message;
  ui.error.hidden = false;
}

function setView(next: View): void {
  view = next;
  ui.showChart.setAttribute("aria-pressed", String(next === "chart"));
  ui.showTable.setAttribute("aria-pressed", String(next === "table"));
  ui.chart.hidden = next !== "chart";
  ui.table.hidden = next !== "table";
}

function describe(current: ChartPayload): string {
  const rows = `${current.row_count.toLocaleString()}${current.truncated ? "+" : ""} rows`;
  const when = new Date(current.executed_at);
  const time = Number.isNaN(when.getTime()) ? current.executed_at : when.toLocaleString();
  return `${current.source} · ${rows} · ${current.elapsed_ms} ms · queried ${time}`;
}

async function drawChart(): Promise<void> {
  if (!payload) return;
  embedded?.finalize();
  embedded = null;
  try {
    embedded = await renderChart(ui.chart, payload, theme);
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    showError(`The chart could not be drawn (${reason}). The table below shows the data.`);
    setView("table");
  }
}

async function show(next: ChartPayload): Promise<void> {
  payload = next;
  ui.error.hidden = true;
  ui.summary.textContent = next.summary;
  ui.meta.textContent = describe(next);
  ui.sql.textContent = next.display_sql;
  ui.notes.replaceChildren(
    ...next.notes.map((note) => {
      const item = document.createElement("li");
      item.textContent = note;
      return item;
    }),
  );
  ui.notes.hidden = next.notes.length === 0;
  renderTable(ui.table, next);
  setView(view);
  await drawChart();
}

function applyContext(context: Partial<McpUiHostContext> | undefined): void {
  if (!context) return;
  if (context.theme && context.theme !== theme) {
    theme = context.theme;
    applyDocumentTheme(theme);
    void drawChart();
  }
  if (context.styles?.variables) applyHostStyleVariables(context.styles.variables);
  if (context.styles?.css?.fonts) applyHostFonts(context.styles.css.fonts);
  if (context.availableDisplayModes) {
    ui.fullscreen.hidden = !context.availableDisplayModes.includes("fullscreen");
  }
  if (context.displayMode) {
    fullscreen = context.displayMode === "fullscreen";
    ui.fullscreen.textContent = fullscreen ? "Collapse" : "Expand";
  }
}

ui.showChart.addEventListener("click", () => setView("chart"));
ui.showTable.addEventListener("click", () => setView("table"));
ui.fullscreen.addEventListener("click", () => {
  void app.requestDisplayMode({ mode: fullscreen ? "inline" : "fullscreen" });
});

// Handlers must be registered before connect(), or early notifications are missed.
app.ontoolresult = (result) => {
  const parsed = parsePayload(result.structuredContent);
  if (parsed) {
    void show(parsed);
    return;
  }
  const text = result.content?.find((block) => block.type === "text");
  showError(text && "text" in text ? String(text.text) : "The tool returned no chart data.");
  ui.summary.textContent = "No chart";
};
app.onhostcontextchanged = (context) => applyContext(context);

app
  .connect()
  .then(() => applyContext(app.getHostContext()))
  .catch((error: unknown) => {
    const reason = error instanceof Error ? error.message : String(error);
    ui.summary.textContent = "Could not connect to the host";
    showError(`This chart view could not start (${reason}).`);
  });
