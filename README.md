# Data BI MCP Server

A Model Context Protocol (MCP) server that transforms data into high-quality Business Intelligence (BI) charts. This server enables AI assistants to connect to various data sources, perform data transformations, and generate visualizations through natural language requests.

## What is MCP?

The Model Context Protocol (MCP) is an open standard that enables secure, two-way connections between AI models and external systems. This server implements MCP to allow AI assistants like Claude to interact with your data and generate visualizations.

## Features

- **Data Source Connections**: Connect to databases (PostgreSQL, MySQL, SQLite) and files (CSV, Excel, JSON)
- **Data Transformation**: Filter, aggregate, join, and sort data
- **BI Visualizations**: Generate bar charts, line charts, pie charts, and scatter plots
- **MCP Compatibility**: Works with any MCP-compatible AI assistant

## Installation

``` Bash
Clone the repository
git clone https://github.com/yourusername/data-bi-mcp-server.git
cd data-bi-mcp-server
```

### Install dependencies
``` Bash
npm install
```
### Build the project
``` Bash
npm run build
```


## Usage

### Starting the Server

``` Bash
npm start
```


The server will start on port 9000 by default. You can configure the port by setting the `PORT` environment variable.

### Connecting from an MCP-Compatible AI Assistant

To connect to the server from an MCP-enabled AI assistant (like Claude Desktop), use:

- **Protocol**: HTTP+SSE
- **Endpoint**: `http://localhost:9000`

### Example Interactions

Once connected, you can use natural language to:

- "Connect to my PostgreSQL database at localhost:5432"
- "Load the sales data from the CSV file at /data/sales.csv"
- "Filter the data to show only transactions over $1000"
- "Create a bar chart showing monthly sales by region"
- "Generate a pie chart showing product category distribution"

## Development

### Project Structure

- `src/connectors/`: Data source connection modules
- `src/transformers/`: Data transformation modules
- `src/visualizers/`: Chart generation modules
- `src/mcp/`: Model Context Protocol implementation

### Adding New Features

The modular architecture makes it easy to extend the server with new capabilities:

- **New Data Sources**: Add a new connector in `src/connectors/`
- **New Transformations**: Add a new transformer in `src/transformers/`
- **New Visualizations**: Add a new visualizer in `src/visualizers/`

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

