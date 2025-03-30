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
async def test_server_initialization(server):
    assert server.app.name == "mcp-bi-visualizer"

@pytest.mark.asyncio
async def test_load_data_tool_registration(server):
    tool = server.app.get_tool("load_data")
    assert tool is not None
    assert tool.name == "load_data"