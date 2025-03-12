import asyncio
import os
import json
from typing import Dict, List, Optional, Any

import mcp.types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server

from .data.loader import DataLoader
from .data.processor import DataProcessor
from .visualization.vega_lite import VegaLiteGenerator
from .visualization.renderer import VisualizationRenderer
from .resources.manager import ResourceManager

class BiVisualizerServer:
    """
    MCP server for generating BI visualizations from data.
    """
    
    def __init__(self, name: str = "mcp-bi-visualizer"):
        """
        Initialize the BI Visualizer MCP server.
        
        Args:
            name: Name of the MCP server
        """
        self.app = Server(name)
        self.data_loader = DataLoader()
        self.data_processor = DataProcessor()
        self.vega_generator = VegaLiteGenerator()
        self.renderer = VisualizationRenderer()
        self.resource_manager = ResourceManager()
        
        # Register MCP handlers
        self.register_tools()
        self.register_resources()
        self.register_prompts()
    
    def register_tools(self):
        """Register all MCP tools."""
        
        @self.app.tool("load_data")
        async def load_data(source: str, format: str = "auto") -> Dict[str, Any]:
            """
            Load data from the specified source.
            
            Args:
                source: Path or URL to the data
                format: Data format (auto, csv, json, sql)
                
            Returns:
                Dictionary with data information and preview
            """
            data = await self.data_loader.load(source, format)
            preview = data.head(5).to_dict() if hasattr(data, 'head') else data[:5]
            
            return {
                "success": True,
                "rows": len(data),
                "columns": list(data.columns) if hasattr(data, 'columns') else list(data[0].keys()),
                "preview": preview
            }
        
        @self.app.tool("create_visualization")
        async def create_visualization(
            data_source: str,
            chart_type: str,
            x_axis: str,
            y_axis: str,
            options: Optional[Dict[str, Any]] = None
        ) -> Dict[str, Any]:
            """
            Create a visualization from the given data.
            
            Args:
                data_source: Path or URL to the data
                chart_type: Type of chart (bar, line, scatter, pie, etc.)
                x_axis: Column to use for x-axis
                y_axis: Column to use for y-axis
                options: Additional visualization options
                
            Returns:
                Dictionary with visualization URI and metadata
            """
            # Load data
            data = await self.data_loader.load(data_source)
            
            # Process data for visualization
            processed_data = self.data_processor.prepare_for_visualization(
                data, x_axis, y_axis, options
            )
            
            # Generate Vega-Lite spec
            vega_spec = self.vega_generator.create_chart(
                chart_type, processed_data, x_axis, y_axis, options
            )
            
            # Render visualization
            image_data = await self.renderer.render(vega_spec, format="png")
            
            # Save as resource
            resource_uri = await self.resource_manager.save_visualization(
                chart_type, vega_spec, image_data
            )
            
            return {
                "success": True,
                "visualization_uri": resource_uri,
                "spec": vega_spec
            }
        
        @self.app.tool("add_insight")
        async def add_insight(visualization_uri: str, insight: str) -> Dict[str, Any]:
            """
            Add a business insight about a visualization to the memo.
            
            Args:
                visualization_uri: URI of the visualization
                insight: Text describing the business insight
                
            Returns:
                Dictionary with success status
            """
            await self.resource_manager.add_insight(visualization_uri, insight)
            return {
                "success": True,
                "message": "Insight added to memo"
            }
    
    def register_resources(self):
        """Register MCP resources."""
        
        @self.app.list_resources()
        async def list_resources() -> List[types.Resource]:
            """
            List all available resources.
            
            Returns:
                List of MCP resources
            """
            return await self.resource_manager.list_resources()
        
        @self.app.get_resource()
        async def get_resource(uri: str) -> types.ResourceContent:
            """
            Get resource content by URI.
            
            Args:
                uri: Resource URI
                
            Returns:
                Resource content
            """
            return await self.resource_manager.get_resource(uri)
    
    def register_prompts(self):
        """Register MCP prompts."""
        
        @self.app.list_prompts()
        async def list_prompts() -> List[types.Prompt]:
            """
            List available prompts.
            
            Returns:
                List of MCP prompts
            """
            return [
                types.Prompt(
                    name="create-dashboard",
                    description="Create a BI dashboard from your data",
                    arguments=[
                        types.PromptArgument(
                            name="data_source",
                            description="Path or URL to your data",
                            required=True
                        ),
                        types.PromptArgument(
                            name="business_question",
                            description="Business question to answer with visualizations",
                            required=True
                        )
                    ]
                ),
                types.Prompt(
                    name="analyze-trends",
                    description="Analyze trends in time-series data",
                    arguments=[
                        types.PromptArgument(
                            name="data_source",
                            description="Path or URL to your data",
                            required=True
                        ),
                        types.PromptArgument(
                            name="time_column",
                            description="Column containing time/date information",
                            required=True
                        ),
                        types.PromptArgument(
                            name="value_column",
                            description="Column containing values to analyze",
                            required=True
                        )
                    ]
                )
            ]
        
        @self.app.get_prompt()
        async def get_prompt(name: str, arguments: Optional[Dict[str, str]] = None) -> types.GetPromptResult:
            """
            Get prompt by name.
            
            Args:
                name: Prompt name
                arguments: Prompt arguments
                
            Returns:
                Prompt result with messages
            """
            if name == "create-dashboard":
                data_source = arguments.get("data_source", "")
                business_question = arguments.get("business_question", "")
                
                return types.GetPromptResult(
                    messages=[
                        types.Message(
                            role="user",
                            content=types.TextContent(
                                type="text",
                                text=f"I want to create a BI dashboard to answer this business question: '{business_question}'. "
                                     f"Please help me analyze the data from {data_source} and create appropriate visualizations."
                            )
                        )
                    ]
                )
            
            elif name == "analyze-trends":
                data_source = arguments.get("data_source", "")
                time_column = arguments.get("time_column", "")
                value_column = arguments.get("value_column", "")
                
                return types.GetPromptResult(
                    messages=[
                        types.Message(
                            role="user",
                            content=types.TextContent(
                                type="text",
                                text=f"I want to analyze trends in my time-series data from {data_source}. "
                                     f"Please analyze the '{time_column}' column as my time dimension and the '{value_column}' column as my values."
                            )
                        )
                    ]
                )
            
            raise ValueError(f"Unknown prompt: {name}")
    
    async def run(self):
        """Run the MCP server."""
        async with stdio_server() as streams:
            await self.app.run(
                streams[0],
                streams[1],
                self.app.create_initialization_options()
            )

async def main():
    """Main entry point for the server."""
    server = BiVisualizerServer()
    await server.run()

if __name__ == "__main__":
    asyncio.run(main())
