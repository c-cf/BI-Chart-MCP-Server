/**
 * A minimal MCP Apps host for developing the chart view without Claude Desktop.
 * Loads ../chart.html in an iframe and sends it a recorded `visualize` result from
 * ./samples (generate them with `uv run python scripts/make_ui_samples.py`).
 */
import { AppBridge, PostMessageTransport } from "@modelcontextprotocol/ext-apps/app-bridge";

type ToolResult = Parameters<AppBridge["sendToolResult"]>[0];

const samples = import.meta.glob<ToolResult>("./samples/*.json", { eager: true, import: "default" });

const select = document.getElementById("sample") as HTMLSelectElement;
const dark = document.getElementById("dark") as HTMLInputElement;
const frame = document.getElementById("frame") as HTMLDivElement;

for (const path of Object.keys(samples).sort()) {
  const option = document.createElement("option");
  option.value = path;
  option.textContent = path.replace("./samples/", "").replace(".json", "");
  select.appendChild(option);
}

const params = new URLSearchParams(location.search);
if (params.get("sample")) select.value = `./samples/${params.get("sample")}.json`;
dark.checked = params.get("theme") === "dark";

let bridge: AppBridge | null = null;

async function mount(): Promise<void> {
  const theme = dark.checked ? "dark" : "light";
  document.body.classList.toggle("dark", dark.checked);
  await bridge?.close();
  const iframe = document.createElement("iframe");
  frame.replaceChildren(iframe);
  const window = iframe.contentWindow;
  if (!window) throw new Error("iframe has no window");

  const current = new AppBridge(
    null,
    { name: "bi-chart-dev-host", version: "0.0.0" },
    { serverTools: {} },
    { hostContext: { theme, displayMode: "inline", availableDisplayModes: ["inline", "fullscreen"] } },
  );
  current.onsizechange = ({ height }) => {
    if (height) iframe.style.height = `${Math.ceil(height)}px`;
  };
  current.oninitialized = () => {
    const sample = samples[select.value];
    if (sample) void current.sendToolResult(sample);
  };
  current.onrequestdisplaymode = async ({ mode }) => ({ mode });
  // Listen before the view loads, so its ui/initialize request is not missed.
  await current.connect(new PostMessageTransport(window, window));
  bridge = current;
  iframe.src = "/chart.html";
}

select.addEventListener("change", () => void mount());
dark.addEventListener("change", () => void mount());
void mount();
