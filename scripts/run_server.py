import asyncio
from mcp_bi_visualizer.server import BiVisualizerServer

async def main():
    """Main entry point for the MCP server."""
    server = BiVisualizerServer()
    await server.run()

if __name__ == "__main__":
    asyncio.run(main())