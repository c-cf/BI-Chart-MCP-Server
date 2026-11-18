import { resolve } from "node:path";

import { defineConfig } from "vitest/config";
import { viteSingleFile } from "vite-plugin-singlefile";

// Builds chart.html into one self-contained file (all JS and CSS inlined) that the
// Python server serves as the `ui://bi-chart/chart.html` MCP Apps resource.
export default defineConfig({
  plugins: [viteSingleFile()],
  build: {
    outDir: resolve(__dirname, "../src/bi_chart_mcp/ui"),
    emptyOutDir: false,
    rollupOptions: { input: resolve(__dirname, "chart.html") },
    sourcemap: false,
    reportCompressedSize: false,
  },
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
  },
});
