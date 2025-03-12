# 定義目錄結構
$directories = @(
    ".github/workflows",
    "mcp_bi_visualizer",
    "mcp_bi_visualizer/data",
    "mcp_bi_visualizer/visualization",
    "mcp_bi_visualizer/resources",
    "scripts",
    "tests",
    "examples",
    "data"
)

# 定義檔案結構
$files = @(
    "mcp_bi_visualizer/__init__.py",
    "mcp_bi_visualizer/server.py",
    "mcp_bi_visualizer/data/__init__.py",
    "mcp_bi_visualizer/data/loader.py",
    "mcp_bi_visualizer/data/processor.py",
    "mcp_bi_visualizer/visualization/__init__.py",
    "mcp_bi_visualizer/visualization/vega_lite.py",
    "mcp_bi_visualizer/visualization/renderer.py",
    "mcp_bi_visualizer/resources/__init__.py",
    "mcp_bi_visualizer/resources/manager.py",
    "mcp_bi_visualizer/resources/memo.py",
    "mcp_bi_visualizer/config.py",
    "scripts/run_server.py",
    "tests/test_server.py",
    "tests/test_visualization.py",
    "examples/basic_chart.py",
    "data/sample.csv",
    ".github/workflows/test.yml",
    ".gitignore",
    "LICENSE",
    "README.md",
    "CONTRIBUTING.md",
    "setup.py",
    "requirements.txt"
)

# 建立目錄
foreach ($dir in $directories) {
    if (!(Test-Path -Path $dir)) {
        New-Item -ItemType Directory -Path $dir | Out-Null
    }
}

# 建立檔案（若不存在則建立空檔案）
foreach ($file in $files) {
    if (!(Test-Path -Path $file)) {
        New-Item -ItemType File -Path $file | Out-Null
    }
}
