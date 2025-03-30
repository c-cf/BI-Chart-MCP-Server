import pytest
import asyncio
from mcp_bi_visualizer.server import BiVisualizerServer

@pytest.fixture
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()

@pytest.fixture
async def server():
    server = BiVisualizerServer()
    await server.app.start()
    yield server
    await server.app.stop()

@pytest.mark.asyncio
async def test_create_visualization(server):
    # Test data
    data_source = "data/sample.csv"
    chart_type = "bar"
    x_axis = "date"
    y_axis = "value"
    options = {"title": "Sample Bar Chart"}

    # Create visualization
    result = await server.app.get_tool("create_visualization")(
        data_source=data_source,
        chart_type=chart_type,
        x_axis=x_axis,
        y_axis=y_axis,
        options=options
    )

    # Assertions
    assert result["success"] is True
    assert "visualization_uri" in result
    assert "spec" in result

@pytest.mark.asyncio
async def test_add_insight(server):
    # Test data
    visualization_uri = "some/uri/to/visualization"
    insight = "This is a business insight."

    # Add insight
    result = await server.app.get_tool("add_insight")(
        visualization_uri=visualization_uri,
        insight=insight
    )

    # Assertions
    assert result["success"] is True
    assert result["message"] == "Insight added to memo"