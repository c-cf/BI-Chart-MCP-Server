"""
Data loader module for MCP BI Visualizer.
Provides functions to load data from various sources including databases,
files (CSV, Excel, JSON), and more.
"""
import os
import pandas as pd
import numpy as np
import json
import logging
from typing import Dict, Any, Union, Optional, List, Tuple
import sqlalchemy as db
from sqlalchemy.engine import Engine, Connection
from urllib.parse import urlparse
import requests

logger = logging.getLogger(__name__)

class DataLoader:
    """
    Loads data from various sources into pandas DataFrames for BI visualization.
    Supports databases, files, APIs, and direct data input.
    """
    
    @staticmethod
    def load_from_database(connection_params: Dict[str, Any], query: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Load data from database using SQL query
        
        Args:
            connection_params: Database connection parameters including:
                - type: Database type (postgres, mysql, sqlite, etc.)
                - host: Database host (if applicable)
                - port: Database port (if applicable)
                - username: Database username (if applicable)
                - password: Database password (if applicable)
                - database: Database name or path
            query: SQL query to execute
            
        Returns:
            Tuple of (DataFrame with query results, metadata)
        """
        try:
            engine = DataLoader._create_db_engine(connection_params)
            db_type = connection_params.get('type', '').lower()
            
            logger.info(f"Executing query on {db_type} database")
            
            # Execute the query and return results as DataFrame
            df = pd.read_sql(query, engine)
            
            # Create metadata about the dataset
            metadata = {
                "source_type": "database",
                "database_type": db_type,
                "database_name": connection_params.get('database'),
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": list(df.columns),
                "dtypes": {col: str(df[col].dtype) for col in df.columns},
                "query": query
            }
            
            logger.info(f"Query returned {len(df)} rows with {len(df.columns)} columns")
            return df, metadata
            
        except Exception as e:
            logger.error(f"Database query error: {str(e)}")
            raise
    
    @staticmethod
    def load_from_csv(file_path: str, **kwargs) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Load data from CSV file
        
        Args:
            file_path: Path to CSV file
            **kwargs: Additional parameters for pd.read_csv (e.g. encoding, delimiter)
            
        Returns:
            Tuple of (DataFrame with loaded data, metadata)
        """
        try:
            logger.info(f"Loading data from CSV: {file_path}")
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"CSV file not found: {file_path}")
                
            # Read the CSV file
            df = pd.read_csv(file_path, **kwargs)
            
            # Create metadata
            file_stats = os.stat(file_path)
            metadata = {
                "source_type": "file",
                "file_type": "csv",
                "file_path": file_path,
                "file_size_bytes": file_stats.st_size,
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": list(df.columns),
                "dtypes": {col: str(df[col].dtype) for col in df.columns},
                "read_params": kwargs
            }
            
            logger.info(f"Loaded {len(df)} rows from CSV")
            return df, metadata
            
        except Exception as e:
            logger.error(f"CSV loading error: {str(e)}")
            raise
    
    @staticmethod
    def load_from_excel(file_path: str, sheet_name: Optional[str] = None, **kwargs) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Load data from Excel file
        
        Args:
            file_path: Path to Excel file
            sheet_name: Sheet name or index (optional)
            **kwargs: Additional parameters for pd.read_excel
            
        Returns:
            Tuple of (DataFrame with loaded data, metadata)
        """
        try:
            logger.info(f"Loading data from Excel: {file_path}")
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Excel file not found: {file_path}")
                
            # Read the Excel file
            df = pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)
            
            # Handle case where multiple sheets are returned
            if isinstance(df, dict):
                if sheet_name is None:
                    # If no specific sheet was requested, use the first sheet
                    sheet_name = list(df.keys())[0]
                    df = df[sheet_name]
                    logger.info(f"Multiple sheets found, using sheet: {sheet_name}")
                else:
                    # This shouldn't happen normally, but handle it anyway
                    raise ValueError(f"Multiple sheets returned when specific sheet '{sheet_name}' was requested")
            
            # Create metadata
            file_stats = os.stat(file_path)
            metadata = {
                "source_type": "file",
                "file_type": "excel",
                "file_path": file_path,
                "sheet_name": sheet_name,
                "file_size_bytes": file_stats.st_size,
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": list(df.columns),
                "dtypes": {col: str(df[col].dtype) for col in df.columns},
                "read_params": kwargs
            }
            
            logger.info(f"Loaded {len(df)} rows from Excel sheet '{sheet_name}'")
            return df, metadata
            
        except Exception as e:
            logger.error(f"Excel loading error: {str(e)}")
            raise
    
    @staticmethod
    def load_from_json(json_data: Union[str, Dict, List]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Load data from JSON
        
        Args:
            json_data: JSON string, file path, or Python dict/list
            
        Returns:
            Tuple of (DataFrame with loaded data, metadata)
        """
        try:
            source_info = {}
            
            # If json_data is a file path
            if isinstance(json_data, str) and os.path.exists(json_data):
                logger.info(f"Loading data from JSON file: {json_data}")
                with open(json_data, 'r') as f:
                    data = json.load(f)
                source_info = {
                    "source_type": "file",
                    "file_type": "json",
                    "file_path": json_data,
                    "file_size_bytes": os.stat(json_data).st_size
                }
            
            # If json_data is a JSON string
            elif isinstance(json_data, str):
                logger.info("Parsing JSON string")
                data = json.loads(json_data)
                source_info = {
                    "source_type": "string",
                    "string_length": len(json_data)
                }
            
            # If json_data is already a Python dict/list
            else:
                logger.info("Using provided Python object as JSON data")
                data = json_data
                source_info = {
                    "source_type": "object",
                    "object_type": type(data).__name__
                }
            
            # Convert to DataFrame
            if isinstance(data, list):
                if all(isinstance(item, dict) for item in data):
                    # List of dictionaries
                    df = pd.json_normalize(data)
                else:
                    # Simple list - convert to single column DataFrame
                    df = pd.DataFrame(data, columns=['value'])
            elif isinstance(data, dict):
                # Convert dict to DataFrame
                if all(isinstance(data[k], list) for k in data):
                    # Dict of lists - each key becomes a column
                    df = pd.DataFrame(data)
                else:
                    # Single dict - becomes a single row
                    df = pd.DataFrame([data])
            else:
                raise ValueError(f"Unsupported JSON data structure of type {type(data)}")
            
            # Create metadata
            metadata = {
                **source_info,
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": list(df.columns),
                "dtypes": {col: str(df[col].dtype) for col in df.columns}
            }
            
            logger.info(f"Loaded {len(df)} rows from JSON")
            return df, metadata
            
        except Exception as e:
            logger.error(f"JSON loading error: {str(e)}")
            raise
    
    @staticmethod
    def load_from_api(url: str, method: str = "GET", 
                     headers: Optional[Dict[str, str]] = None,
                     params: Optional[Dict[str, Any]] = None,
                     data: Optional[Any] = None,
                     json_path: Optional[str] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Load data from a REST API
        
        Args:
            url: API endpoint URL
            method: HTTP method (GET, POST, etc.)
            headers: HTTP headers
            params: URL parameters
            data: Request body data
            json_path: Path to extract from JSON response (e.g. "results.data")
            
        Returns:
            Tuple of (DataFrame with loaded data, metadata)
        """
        try:
            logger.info(f"Loading data from API: {url}")
            
            # Make the request
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=data if data else None
            )
            
            # Check for successful response
            response.raise_for_status()
            
            # Parse JSON response
            json_data = response.json()
            
            # Extract data from specified path if provided
            if json_path:
                parts = json_path.split('.')
                for part in parts:
                    json_data = json_data[part]
            
            # Convert to DataFrame using json loader
            df, json_metadata = DataLoader.load_from_json(json_data)
            
            # Create API-specific metadata
            parsed_url = urlparse(url)
            api_metadata = {
                "source_type": "api",
                "url": url,
                "method": method,
                "domain": parsed_url.netloc,
                "status_code": response.status_code,
                "response_time_ms": response.elapsed.total_seconds() * 1000,
                "json_path": json_path
            }
            
            # Combine metadata
            metadata = {**api_metadata, **{k:v for k,v in json_metadata.items() if k not in ["source_type"]}}
            
            logger.info(f"API request returned {len(df)} rows with {len(df.columns)} columns")
            return df, metadata
            
        except Exception as e:
            logger.error(f"API loading error: {str(e)}")
            raise
    
    @staticmethod
    def _create_db_engine(connection_params: Dict[str, Any]) -> Engine:
        """
        Create a SQLAlchemy engine from connection parameters
        
        Args:
            connection_params: Database connection parameters
            
        Returns:
            SQLAlchemy engine
        """
        db_type = connection_params.get('type', '').lower()
        
        if db_type == 'postgres' or db_type == 'postgresql':
            conn_str = (f"postgresql://{connection_params.get('username')}:{connection_params.get('password')}"
                     f"@{connection_params.get('host')}:{connection_params.get('port', 5432)}"
                     f"/{connection_params.get('database')}")
                     
        elif db_type == 'mysql':
            conn_str = (f"mysql+pymysql://{connection_params.get('username')}:{connection_params.get('password')}"
                     f"@{connection_params.get('host')}:{connection_params.get('port', 3306)}"
                     f"/{connection_params.get('database')}")
                     
        elif db_type == 'sqlite':
            db_path = connection_params.get('database')
            conn_str = f"sqlite:///{db_path}"
            
        elif db_type == 'mssql' or db_type == 'sqlserver':
            conn_str = (f"mssql+pyodbc://{connection_params.get('username')}:{connection_params.get('password')}"
                     f"@{connection_params.get('host')}:{connection_params.get('port', 1433)}"
                     f"/{connection_params.get('database')}?driver=ODBC+Driver+17+for+SQL+Server")
                     
        elif db_type == 'oracle':
            dsn = connection_params.get('dsn', 
                f"{connection_params.get('host')}:{connection_params.get('port', 1521)}/{connection_params.get('service_name')}")
            conn_str = (f"oracle+cx_oracle://{connection_params.get('username')}:{connection_params.get('password')}"
                     f"@{dsn}")
            
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
            
        return db.create_engine(conn_str)
