# BI Chart MCP Server

This project implements the BI Chart MCP Server using Python. Previously, the functionality was prototyped with TypeScript, but the current and maintained version is built with Python.

## Project Structure

- **mcp_bi_visualizer/**: Contains the main server code and modules.
  - `server.py`: Main entry point for starting the MCP server.
  - **data/**: Data loading and processing modules.
    - `loader.py`
    - `processor.py`
  - **resources/**: Modules to manage project resources.
    - `manager.py`
    - `memo.py`
  - **visualization/**: Visualization components.
    - `renderer.py`
    - `vega_lite.py`
- **scripts/**
  - `run_server.py`: A script to launch the server.
- **tests/**: Unit tests for the server and visualization components.
- Other files include configuration files (e.g., `pyproject.toml`, `requirements.txt`, `setup.py`) and documentation.

## Installation

1. Clone the repository.
2. Create a virtual environment and activate it:
   ```
   python -m venv .venv
   .venv\Scripts\activate   # On Windows
   ```
3. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Running the Server

You can run the server using the provided script:

```
python scripts/run_server.py
```

Alternatively, you can start the server directly from the module:

```
python -m mcp_bi_visualizer.server
```

## Testing

Run the tests using your preferred test runner. For example, with pytest:

```
pytest
```

## Notes

- The project has been migrated from a TypeScript-based implementation to Python.
- For any issues or contributions, please refer to the [CONTRIBUTING.md](CONTRIBUTING.md) file.

Enjoy using the BI Chart MCP Server!
