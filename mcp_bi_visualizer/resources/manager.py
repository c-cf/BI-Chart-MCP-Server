"""
Resource Manager for MCP BI Visualizer.
Manages connections, datasets, and visualizations as resources.
"""
import uuid
import logging
import json
import os
from datetime import datetime
from typing import Dict, Any, Optional, List, Union

logger = logging.getLogger(__name__)

class ResourceManager:
    """
    Manages all resources for the MCP BI server, including:
    - Data source connections
    - Datasets and transformation histories
    - Visualizations and their configurations
    """
    
    def __init__(self, storage_dir: str = "storage"):
        """
        Initialize the resource manager with storage for persistent resources.
        
        Args:
            storage_dir: Directory to store persistent resources
        """
        self.connections = {}  # Store active connections by ID
        self.datasets = {}     # Store loaded datasets by ID
        self.visualizations = {}  # Store generated visualizations by ID
        
        # Ensure storage directories exist
        self.storage_dir = storage_dir
        self.viz_dir = os.path.join(storage_dir, "visualizations")
        self.data_dir = os.path.join(storage_dir, "datasets")
        
        os.makedirs(self.viz_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)
        
        logger.info(f"Initialized Resource Manager with storage in {storage_dir}")
    
    def register_connection(self, connection_type: str, connection_params: Dict[str, Any]) -> str:
        """
        Register a new data source connection
        
        Args:
            connection_type: Type of connection (database, file, api, etc.)
            connection_params: Connection parameters
            
        Returns:
            Connection ID
        """
        connection_id = str(uuid.uuid4())
        
        # Create a sanitized version of connection params for logging (remove passwords)
        safe_params = {k: v if k != 'password' else '********' 
                      for k, v in connection_params.items()}
        
        self.connections[connection_id] = {
            "id": connection_id,
            "type": connection_type,
            "params": connection_params,
            "safe_params": safe_params,
            "status": "created",
            "created_at": self._get_timestamp()
        }
        
        logger.info(f"Registered new {connection_type} connection with ID: {connection_id}")
        return connection_id
    
    def update_connection_status(self, connection_id: str, status: str, error: Optional[str] = None) -> bool:
        """
        Update the status of a connection
        
        Args:
            connection_id: The connection ID
            status: New status ('connected', 'error', 'closed')
            error: Error message if status is 'error'
            
        Returns:
            True if updated, False if connection not found
        """
        if connection_id not in self.connections:
            return False
            
        self.connections[connection_id]["status"] = status
        
        if error:
            self.connections[connection_id]["error"] = error
            
        logger.info(f"Updated connection {connection_id} status to {status}")
        return True
    
    def get_connection(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """
        Get connection details by ID
        
        Args:
            connection_id: The connection ID
            
        Returns:
            Connection details or None if not found
        """
        return self.connections.get(connection_id)
    
    def list_connections(self) -> List[Dict[str, Any]]:
        """
        List all connections with sanitized information
        
        Returns:
            List of connection information (without sensitive data)
        """
        return [
            {
                "id": conn_id,
                "type": conn["type"],
                "params": conn.get("safe_params", {}),
                "status": conn["status"],
                "created_at": conn["created_at"]
            }
            for conn_id, conn in self.connections.items()
        ]
    
    def register_dataset(self, data: Any, metadata: Dict[str, Any], 
                         source_connection_id: Optional[str] = None) -> str:
        """
        Register a dataset
        
        Args:
            data: The dataset
            metadata: Dataset metadata (source, schema, etc.)
            source_connection_id: ID of the connection that provided this data
            
        Returns:
            Dataset ID
        """
        dataset_id = str(uuid.uuid4())
        created_at = self._get_timestamp()
        
        # Prepare dataset record
        dataset_record = {
            "id": dataset_id,
            "data": data,
            "metadata": metadata,
            "source_connection_id": source_connection_id,
            "transformations": [],
            "created_at": created_at
        }
        
        # Store in memory
        self.datasets[dataset_id] = dataset_record
        
        # Log dataset creation
        row_count = len(data) if hasattr(data, "__len__") else "unknown"
        logger.info(f"Registered dataset with ID: {dataset_id}, containing {row_count} records")
        
        return dataset_id
    
    def add_transformation(self, dataset_id: str, transformation_type: str, 
                          transformation_params: Dict[str, Any]) -> bool:
        """
        Add a transformation record to a dataset's history
        
        Args:
            dataset_id: The dataset ID
            transformation_type: Type of transformation (filter, aggregate, etc.)
            transformation_params: Parameters of the transformation
            
        Returns:
            True if added, False if dataset not found
        """
        if dataset_id not in self.datasets:
            return False
            
        self.datasets[dataset_id]["transformations"].append({
            "type": transformation_type,
            "params": transformation_params,
            "applied_at": self._get_timestamp()
        })
        
        logger.info(f"Added {transformation_type} transformation to dataset {dataset_id}")
        return True
    
    def update_dataset(self, dataset_id: str, new_data: Any) -> bool:
        """
        Update a dataset with new data (after transformation)
        
        Args:
            dataset_id: The dataset ID
            new_data: The new dataset
            
        Returns:
            True if updated, False if dataset not found
        """
        if dataset_id not in self.datasets:
            return False
            
        self.datasets[dataset_id]["data"] = new_data
        logger.info(f"Updated dataset {dataset_id} with new data")
        return True
    
    def get_dataset(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """
        Get dataset by ID
        
        Args:
            dataset_id: The dataset ID
            
        Returns:
            Dataset or None if not found
        """
        return self.datasets.get(dataset_id)
    
    def register_visualization(self, dataset_id: str, chart_type: str, 
                              config: Dict[str, Any], image_path: str) -> str:
        """
        Register a visualization
        
        Args:
            dataset_id: Source dataset ID
            chart_type: Type of chart (bar, line, pie, etc.)
            config: Chart configuration
            image_path: Path to the generated image
            
        Returns:
            Visualization ID
        """
        viz_id = str(uuid.uuid4())
        created_at = self._get_timestamp()
        
        # Prepare visualization record
        self.visualizations[viz_id] = {
            "id": viz_id,
            "dataset_id": dataset_id,
            "chart_type": chart_type,
            "config": config,
            "image_path": image_path,
            "created_at": created_at
        }
        
        logger.info(f"Registered {chart_type} visualization with ID: {viz_id}")
        return viz_id
    
    def get_visualization(self, viz_id: str) -> Optional[Dict[str, Any]]:
        """
        Get visualization by ID
        
        Args:
            viz_id: The visualization ID
            
        Returns:
            Visualization or None if not found
        """
        return self.visualizations.get(viz_id)
    
    def list_visualizations(self, dataset_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List visualizations, optionally filtered by dataset
        
        Args:
            dataset_id: If provided, only return visualizations for this dataset
            
        Returns:
            List of visualization information
        """
        if not dataset_id:
            return list(self.visualizations.values())
            
        return [v for v in self.visualizations.values() if v["dataset_id"] == dataset_id]
    
    def export_visualization_config(self, viz_id: str, filepath: Optional[str] = None) -> Optional[str]:
        """
        Export visualization configuration to a JSON file
        
        Args:
            viz_id: The visualization ID
            filepath: Optional custom filepath, otherwise one will be generated
            
        Returns:
            Path to the exported file, or None if visualization not found
        """
        viz = self.get_visualization(viz_id)
        if not viz:
            return None
            
        # Generate filename if not provided
        if not filepath:
            filename = f"viz_config_{viz_id}.json"
            filepath = os.path.join(self.viz_dir, filename)
            
        # Export configuration
        with open(filepath, 'w') as f:
            json.dump({
                "id": viz["id"],
                "chart_type": viz["chart_type"],
                "config": viz["config"],
                "created_at": viz["created_at"]
            }, f, indent=2)
            
        logger.info(f"Exported visualization {viz_id} configuration to {filepath}")
        return filepath
    
    def _get_timestamp(self) -> str:
        """Get current timestamp as ISO format string."""
        return datetime.now().isoformat()
    
    def cleanup_resources(self, older_than_days: Optional[int] = None) -> Dict[str, int]:
        """
        Cleanup old resources
        
        Args:
            older_than_days: If provided, remove resources older than this many days
            
        Returns:
            Count of resources cleaned up by type
        """
        # Implementation would check dates and remove old resources
        # For now just return placeholder counts
        return {
            "connections": 0,
            "datasets": 0,
            "visualizations": 0
        }
