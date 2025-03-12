"""
Data processor module for MCP BI Visualizer.
Provides functions for data transformation, cleaning, and preparation
before visualization.
"""
import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List, Union, Optional, Callable, Tuple

logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Processes DataFrames to prepare data for visualization.
    Implements common data transformation, aggregation, and cleaning operations.
    """
    
    @staticmethod
    def filter_data(df: pd.DataFrame, filters: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Filter a DataFrame based on conditions
        
        Args:
            df: Input DataFrame
            filters: List of filter conditions, each with:
                - column: Column name
                - operator: Comparison operator (=, !=, >, <, >=, <=, contains, in, between)
                - value: Value to compare against
                
        Returns:
            Filtered DataFrame
        """
        if not filters:
            return df
            
        filtered_df = df.copy()
        original_row_count = len(filtered_df)
        
        for filter_condition in filters:
            column = filter_condition.get('column')
            operator = filter_condition.get('operator')
            value = filter_condition.get('value')
            
            if column not in df.columns:
                logger.warning(f"Column '{column}' not found in DataFrame, skipping filter")
                continue
                
            try:
                if operator == '=' or operator == '==':
                    filtered_df = filtered_df[filtered_df[column] == value]
                elif operator == '!=':
                    filtered_df = filtered_df[filtered_df[column] != value]
                elif operator == '>':
                    filtered_df = filtered_df[filtered_df[column] > value]
                elif operator == '<':
                    filtered_df = filtered_df[filtered_df[column] < value]
                elif operator == '>=':
                    filtered_df = filtered_df[filtered_df[column] >= value]
                elif operator == '<=':
                    filtered_df = filtered_df[filtered_df[column] <= value]
                elif operator.lower() == 'contains':
                    filtered_df = filtered_df[filtered_df[column].astype(str).str.contains(str(value), na=False)]
                elif operator.lower() == 'notcontains':
                    filtered_df = filtered_df[~filtered_df[column].astype(str).str.contains(str(value), na=False)]
                elif operator.lower() == 'startswith':
                    filtered_df = filtered_df[filtered_df[column].astype(str).str.startswith(str(value), na=False)]
                elif operator.lower() == 'endswith':
                    filtered_df = filtered_df[filtered_df[column].astype(str).str.endswith(str(value), na=False)]
                elif operator.lower() == 'in':
                    if not isinstance(value, list):
                        value = [value]
                    filtered_df = filtered_df[filtered_df[column].isin(value)]
                elif operator.lower() == 'notin':
                    if not isinstance(value, list):
                        value = [value]
                    filtered_df = filtered_df[~filtered_df[column].isin(value)]
                elif operator.lower() == 'between':
                    if not isinstance(value, list) or len(value) != 2:
                        raise ValueError("Between operator requires a list of two values [min, max]")
                    filtered_df = filtered_df[(filtered_df[column] >= value[0]) & (filtered_df[column] <= value[1])]
                elif operator.lower() == 'isnull':
                    filtered_df = filtered_df[filtered_df[column].isnull()]
                elif operator.lower() == 'notnull':
                    filtered_df = filtered_df[filtered_df[column].notnull()]
                else:
                    logger.warning(f"Unsupported operator '{operator}', skipping filter")
                    
            except Exception as e:
                logger.error(f"Error applying filter on column '{column}': {str(e)}")
                
        new_row_count = len(filtered_df)
        logger.info(f"Filtered data from {original_row_count} to {new_row_count} rows ({original_row_count - new_row_count} rows removed)")
        return filtered_df
    
    @staticmethod
    def aggregate_data(df: pd.DataFrame, 
                       group_by: Union[str, List[str]], 
                       aggregations: Dict[str, str]) -> pd.DataFrame:
        """
        Aggregate data by grouping and applying aggregation functions
        
        Args:
            df: Input DataFrame
            group_by: Column(s) to group by
            aggregations: Dictionary mapping columns to aggregation functions
                Example: {'sales': 'sum', 'price': 'mean'}
                
        Returns:
            Aggregated DataFrame
        """
        if not group_by or not aggregations:
            return df
            
        try:
            # Ensure group_by is a list
            if isinstance(group_by, str):
                group_by = [group_by]
                
            # Check if all columns exist
            for col in group_by:
                if col not in df.columns:
                    raise ValueError(f"Group by column '{col}' not found in DataFrame")
                    
            for col in aggregations.keys():
                if col not in df.columns:
                    raise ValueError(f"Aggregation column '{col}' not found in DataFrame")
            
            logger.info(f"Aggregating data by {group_by} with aggregations: {aggregations}")
            original_row_count = len(df)
            
            result = df.groupby(group_by).agg(aggregations).reset_index()
            
            # Flatten multi-index columns if created by aggregation
            if isinstance(result.columns, pd.MultiIndex):
                result.columns = ['_'.join(col).strip('_') for col in result.columns.values]
                
            new_row_count = len(result)
            logger.info(f"Aggregated from {original_row_count} to {new_row_count} rows")
            return result
            
        except Exception as e:
            logger.error(f"Aggregation error: {str(e)}")
            raise
    
    @staticmethod
    def join_data(left_df: pd.DataFrame, 
                 right_df: pd.DataFrame, 
                 left_on: Union[str, List[str]], 
                 right_on: Union[str, List[str]], 
                 join_type: str = 'inner',
                 suffix: Tuple[str, str] = ('_x', '_y')) -> pd.DataFrame:
        """
        Join two DataFrames
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            left_on: Column(s) from left DataFrame to join on
            right_on: Column(s) from right DataFrame to join on
            join_type: Type of join ('inner', 'left', 'right', 'outer')
            suffix: Suffixes to apply to overlapping column names
            
        Returns:
            Joined DataFrame
        """
        # Validate join type
        valid_join_types = ['inner', 'left', 'right', 'outer']
        if join_type not in valid_join_types:
            raise ValueError(f"Invalid join type. Expected one of: {valid_join_types}")
            
        try:
            left_rows = len(left_df)
            right_rows = len(right_df)
            
            logger.info(f"Performing {join_type} join on {left_on} and {right_on}")
            logger.info(f"Left table: {left_rows} rows, Right table: {right_rows} rows")
            
            result = pd.merge(
                left_df, 
                right_df, 
                left_on=left_on, 
                right_on=right_on, 
                how=join_type,
                suffixes=suffix
            )
            
            logger.info(f"Join resulted in {len(result)} rows and {len(result.columns)} columns")
            return result
            
        except Exception as e:
            logger.error(f"Join error: {str(e)}")
            raise
    
    @staticmethod
    def pivot_data(df: pd.DataFrame, 
                  index: Union[str, List[str]], 
                  columns: str, 
                  values: str,
                  aggfunc: Union[str, Callable] = 'mean',
                  fill_value: Optional[Any] = None) -> pd.DataFrame:
        """
        Create a pivot table from DataFrame
        
        Args:
            df: Input DataFrame
            index: Column(s) to use as index
            columns: Column to use for pivot columns
            values: Column containing values
            aggfunc: Aggregation function to apply
            fill_value: Value to use for missing values
            
        Returns:
            Pivot table as DataFrame
        """
        try:
            # Convert string aggfunc to actual function
            if isinstance(aggfunc, str):
                if aggfunc.lower() == 'sum':
                    agg_function = np.sum
                elif aggfunc.lower() == 'mean' or aggfunc.lower() == 'avg':
                    agg_function = np.mean
                elif aggfunc.lower() == 'count':
                    agg_function = len
                elif aggfunc.lower() == 'min':
                    agg_function = np.min
                elif aggfunc.lower() == 'max':
                    agg_function = np.max
                elif aggfunc.lower() == 'median':
                    agg_function = np.median
                elif aggfunc.lower() == 'first':
                    agg_function = lambda x: x.iloc[0]
                elif aggfunc.lower() == 'last':
                    agg_function = lambda x: x.iloc[-1]
                else:
                    raise ValueError(f"Unsupported aggregation function: {aggfunc}")
            else:
                agg_function = aggfunc
            
            logger.info(f"Creating pivot table with index={index}, columns={columns}, values={values}")
            
            pivot_table = pd.pivot_table(
                df, 
                index=index, 
                columns=columns, 
                values=values, 
                aggfunc=agg_function,
                fill_value=fill_value
            ).reset_index()
            
            logger.info(f"Pivot table created with shape {pivot_table.shape}")
            return pivot_table
            
        except Exception as e:
            logger.error(f"Pivot error: {str(e)}")
            raise
    
    @staticmethod
    def sort_data(df: pd.DataFrame, 
                 sort_by: Union[str, List[str]], 
                 ascending: Union[bool, List[bool]] = True) -> pd.DataFrame:
        """
        Sort DataFrame by specified columns
        
        Args:
            df: Input DataFrame
            sort_by: Column(s) to sort by
            ascending: Whether to sort in ascending order (True) or descending (False)
                Can be a list to specify per column
                
        Returns:
            Sorted DataFrame
        """
        try:
            # Ensure sort_by is a list
            if isinstance(sort_by, str):
                sort_by = [sort_by]
                
            # Check if all columns exist
            for col in sort_by:
                if col not in df.columns:
                    raise ValueError(f"Sort column '{col}' not found in DataFrame")
            
            logger.info(f"Sorting data by {sort_by} (ascending={ascending})")
            return df.sort_values(by=sort_by, ascending=ascending)
            
        except Exception as e:
            logger.error(f"Sort error: {str(e)}")
            raise
    
    @staticmethod
    def clean_data(df: pd.DataFrame, 
                  drop_duplicates: bool = False,
                  drop_na: bool = False,
                  drop_columns: Optional[List[str]] = None,
                  rename_columns: Optional[Dict[str, str]] = None,
                  fill_na: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Clean DataFrame by removing duplicates, handling missing values, etc.
        
        Args:
            df: Input DataFrame
            drop_duplicates: Whether to drop duplicate rows
            drop_na: Whether to drop rows with missing values
            drop_columns: List of columns to drop
            rename_columns: Dictionary mapping old column names to new ones
            fill_na: Dictionary mapping column names to values for filling NAs
            
        Returns:
            Cleaned DataFrame
        """
        result_df = df.copy()
        rows_before = len(result_df)
        columns_before = len(result_df.columns)
        
        # Drop duplicate rows
        if drop_duplicates:
            result_df = result_df.drop_duplicates()
            logger.info(f"Dropped {rows_before - len(result_df)} duplicate rows")
        
        # Drop rows with missing values
        if drop_na:
            result_df = result_df.dropna()
            logger.info(f"Dropped rows with missing values, {rows_before - len(result_df)} rows removed")
            
        # Drop specified columns
        if drop_columns:
            valid_columns = [col for col in drop_columns if col in result_df.columns]
            if valid_columns:
                result_df = result_df.drop(columns=valid_columns)
                logger.info(f"Dropped columns: {valid_columns}")
            
            invalid_columns = [col for col in drop_columns if col not in df.columns]
            if invalid_columns:
                logger.warning(f"Columns not found for dropping: {invalid_columns}")
        
        # Rename columns
        if rename_columns:
            # Filter out column names that don't exist
            valid_renames = {old: new for old, new in rename_columns.items() if old in result_df.columns}
            if valid_renames:
                result_df = result_df.rename(columns=valid_renames)
                logger.info(f"Renamed columns: {valid_renames}")
            
            invalid_renames = {old: new for old, new in rename_columns.items() if old not in df.columns}
            if invalid_renames:
                logger.warning(f"Columns not found for renaming: {list(invalid_renames.keys())}")
        
        # Fill missing values
        if fill_na:
            valid_fills = {col: val for col, val in fill_na.items() if col in result_df.columns}
            if valid_fills:
                result_df = result_df.fillna(valid_fills)
                logger.info(f"Filled missing values in columns: {list(valid_fills.keys())}")
            
            invalid_fills = {col: val for col, val in fill_na.items() if col not in df.columns}
            if invalid_fills:
                logger.warning(f"Columns not found for filling NAs: {list(invalid_fills.keys())}")
        
        rows_after = len(result_df)
        columns_after = len(result_df.columns)
        
        logger.info(f"Data cleaning complete: {rows_before}x{columns_before} → {rows_after}x{columns_after}")
        return result_df
    
    @staticmethod
    def transform_columns(df: pd.DataFrame,
                         transformations: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
        """
        Apply transformations to columns
        
        Args:
            df: Input DataFrame
            transformations: Dictionary mapping column names to transformation configs:
                {
                    "column_name": {
                        "type": "transformation_type",
                        "params": {...transformation parameters...}
                    }
                }
                
                Supported transformation types:
                - "datetime_format": Convert string to datetime with specified format
                - "numeric": Convert to numeric, with optional error handling
                - "replace": Replace values
                - "categorical": Convert to categorical type
                - "bin": Bin numeric values into categories
                - "math": Apply mathematical operations
                - "string": Apply string operations
                
        Returns:
            Transformed DataFrame
        """
        result_df = df.copy()
        
        for column, config in transformations.items():
            if column not in result_df.columns:
                logger.warning(f"Column '{column}' not found, skipping transformation")
                continue
                
            transform_type = config.get("type", "").lower()
            params = config.get("params", {})
            
            try:
                if transform_type == "datetime_format":
                    # Convert to datetime
                    date_format = params.get("format")
                    if date_format:
                        result_df[column] = pd.to_datetime(result_df[column], format=date_format)
                    else:
                        result_df[column] = pd.to_datetime(result_df[column], infer_datetime_format=True)
                    
                elif transform_type == "numeric":
                    # Convert to numeric
                    errors = params.get("errors", "coerce")
                    result_df[column] = pd.to_numeric(result_df[column], errors=errors)
                
                elif transform_type == "replace":
                    # Replace values
                    to_replace = params.get("to_replace")
                    value = params.get("value")
                    result_df[column] = result_df[column].replace(to_replace, value)
                
                elif transform_type == "categorical":
                    # Convert to categorical
                    categories = params.get("categories")
                    if categories:
                        result_df[column] = pd.Categorical(result_df[column], categories=categories)
                    else:
                        result_df[column] = result_df[column].astype('category')
                
                elif transform_type == "bin":
                    # Bin numeric data
                    bins = params.get("bins")
                    labels = params.get("labels")
                    if bins:
                        result_df[column] = pd.cut(result_df[column], bins=bins, labels=labels)
                    
                elif transform_type == "string":
                    # String operations
                    string_op = params.get("operation", "").lower()
                    
                    if string_op == "lower":
                        result_df[column] = result_df[column].str.lower()
                    elif string_op == "upper":
                        result_df[column] = result_df[column].str.upper()
                    elif string_op == "strip":
                        result_df[column] = result_df[column].str.strip()
                    elif string_op == "extract":
                        pattern = params.get("pattern")
                        if pattern:
                            result_df[column] = result_df[column].str.extract(pattern, expand=False)
                
                elif transform_type == "math":
                    # Mathematical operations
                    math_op = params.get("operation", "").lower()
                    value = params.get("value")
                    
                    if math_op == "add":
                        result_df[column] = result_df[column] + value
                    elif math_op == "subtract":
                        result_df[column] = result_df[column] - value
                    elif math_op == "multiply":
                        result_df[column] = result_df[column] * value
                    elif math_op == "divide":
                        result_df[column] = result_df[column] / value
                    elif math_op == "log":
                        result_df[column] = np.log(result_df[column])
                    elif math_op == "exp":
                        result_df[column] = np.exp(result_df[column])
                    elif math_op == "round":
                        decimals = params.get("decimals", 0)
                        result_df[column] = result_df[column].round(decimals)
                
                else:
                    logger.warning(f"Unsupported transformation type: {transform_type}")
                    continue
                
                logger.info(f"Applied {transform_type} transformation to column '{column}'")
                
            except Exception as e:
                logger.error(f"Error transforming column '{column}' with {transform_type}: {str(e)}")
        
        return result_df
