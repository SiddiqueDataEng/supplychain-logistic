"""
Silver Layer Processor for Azure Data Lake Storage Gen2
Reads data from Bronze layer, performs transformations, and stores in Silver layer
"""

import os
import time
import logging
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import ResourceExistsError, AzureError
import json
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('silver_layer_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SilverLayerProcessor:
    """
    Silver Layer Processor for Azure Data Lake Storage Gen2
    Handles data transformation from Bronze to Silver layer
    """
    
    def __init__(self, connection_string: str, bronze_container: str = "bronze", silver_container: str = "silver"):
        """
        Initialize the Silver Layer Processor
        
        Args:
            connection_string: Azure Storage connection string
            bronze_container: Name of the bronze container
            silver_container: Name of the silver container
        """
        self.connection_string = connection_string
        self.bronze_container = bronze_container
        self.silver_container = silver_container
        self.blob_service_client = None
        self.bronze_container_client = None
        self.silver_container_client = None
        self.processed_files = {}  # Track processed files
        self.metadata_file = "silver_layer_metadata.json"
        
        # Initialize Azure connection
        self._initialize_azure_connection()
        
        # Load existing metadata
        self._load_metadata()
    
    def _initialize_azure_connection(self):
        """Initialize Azure Storage connection and create containers if needed"""
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(
                self.connection_string
            )
            
            # Create bronze container client
            self.bronze_container_client = self.blob_service_client.get_container_client(
                self.bronze_container
            )
            
            # Create silver container if it doesn't exist
            try:
                self.silver_container_client = self.blob_service_client.create_container(
                    self.silver_container
                )
                logger.info(f"Created new silver container: {self.silver_container}")
            except ResourceExistsError:
                self.silver_container_client = self.blob_service_client.get_container_client(
                    self.silver_container
                )
                logger.info(f"Using existing silver container: {self.silver_container}")
                
        except Exception as e:
            logger.error(f"Failed to initialize Azure connection: {str(e)}")
            raise
    
    def _load_metadata(self):
        """Load processing metadata for change detection"""
        try:
            if os.path.exists(self.metadata_file):
                with open(self.metadata_file, 'r') as f:
                    self.processed_files = json.load(f)
                logger.info(f"Loaded metadata for {len(self.processed_files)} processed files")
        except Exception as e:
            logger.warning(f"Could not load metadata: {str(e)}")
            self.processed_files = {}
    
    def _save_metadata(self):
        """Save processing metadata"""
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(self.processed_files, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save metadata: {str(e)}")
    
    def _download_blob_to_dataframe(self, blob_name: str) -> Optional[pd.DataFrame]:
        """
        Download a blob from bronze container and convert to DataFrame
        
        Args:
            blob_name: Name of the blob to download
            
        Returns:
            DataFrame or None if failed
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.bronze_container,
                blob=blob_name
            )
            
            # Download blob content
            blob_data = blob_client.download_blob().readall()
            
            # Convert to DataFrame
            from io import StringIO
            df = pd.read_csv(StringIO(blob_data.decode('utf-8')))
            
            logger.info(f"Downloaded {blob_name}: {len(df)} rows, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"Failed to download {blob_name}: {str(e)}")
            return None
    
    def _upload_dataframe_to_blob(self, df: pd.DataFrame, blob_name: str, metadata: Dict[str, str] = None) -> bool:
        """
        Upload a DataFrame to silver container as CSV
        
        Args:
            df: DataFrame to upload
            blob_name: Name of the blob
            metadata: Optional metadata to attach
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.silver_container,
                blob=blob_name
            )
            
            # Convert DataFrame to CSV
            csv_data = df.to_csv(index=False)
            
            # Upload with metadata
            upload_metadata = {
                "source_bronze_blob": blob_name,
                "processing_timestamp": datetime.now().isoformat(),
                "row_count": str(len(df)),
                "column_count": str(len(df.columns)),
                "data_types": str(df.dtypes.to_dict())
            }
            
            if metadata:
                upload_metadata.update(metadata)
            
            blob_client.upload_blob(
                csv_data,
                overwrite=True,
                metadata=upload_metadata
            )
            
            logger.info(f"Uploaded to silver: {blob_name} ({len(df)} rows)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload {blob_name}: {str(e)}")
            return False
    
    def _clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean column names by removing special characters and standardizing format
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with cleaned column names
        """
        # Create a copy to avoid modifying original
        df_clean = df.copy()
        
        # Clean column names
        new_columns = []
        for col in df_clean.columns:
            # Convert to lowercase
            clean_col = str(col).lower()
            
            # Replace spaces and special characters with underscores
            clean_col = re.sub(r'[^a-z0-9_]', '_', clean_col)
            
            # Remove multiple consecutive underscores
            clean_col = re.sub(r'_+', '_', clean_col)
            
            # Remove leading/trailing underscores
            clean_col = clean_col.strip('_')
            
            # Ensure column name is not empty
            if not clean_col:
                clean_col = f"column_{len(new_columns)}"
            
            new_columns.append(clean_col)
        
        df_clean.columns = new_columns
        return df_clean
    
    def _standardize_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize data types across the DataFrame
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with standardized data types
        """
        df_clean = df.copy()
        
        for col in df_clean.columns:
            # Skip if column is already properly typed
            if df_clean[col].dtype in ['int64', 'float64', 'bool']:
                continue
            
            # Try to convert to numeric
            try:
                # Check if column contains numeric data
                numeric_series = pd.to_numeric(df_clean[col], errors='coerce')
                if not numeric_series.isna().all():
                    # If most values are numeric, convert
                    if numeric_series.isna().sum() / len(numeric_series) < 0.5:
                        df_clean[col] = numeric_series
                        continue
            except:
                pass
            
            # Try to convert to datetime
            try:
                if 'date' in col.lower() or 'time' in col.lower():
                    datetime_series = pd.to_datetime(df_clean[col], errors='coerce')
                    if not datetime_series.isna().all():
                        df_clean[col] = datetime_series
                        continue
            except:
                pass
            
            # Convert to string and clean
            df_clean[col] = df_clean[col].astype(str)
            df_clean[col] = df_clean[col].str.strip()
            df_clean[col] = df_clean[col].replace(['', 'nan', 'None', 'null'], np.nan)
        
        return df_clean
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate rows from DataFrame
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with duplicates removed
        """
        initial_count = len(df)
        df_clean = df.drop_duplicates()
        removed_count = initial_count - len(df_clean)
        
        if removed_count > 0:
            logger.info(f"Removed {removed_count} duplicate rows")
        
        return df_clean
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values in the DataFrame
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with missing values handled
        """
        df_clean = df.copy()
        
        for col in df_clean.columns:
            missing_count = df_clean[col].isna().sum()
            
            if missing_count > 0:
                logger.info(f"Column {col}: {missing_count} missing values")
                
                # Handle based on data type
                if df_clean[col].dtype in ['int64', 'float64']:
                    # For numeric columns, fill with median
                    median_value = df_clean[col].median()
                    df_clean[col] = df_clean[col].fillna(median_value)
                elif df_clean[col].dtype == 'object':
                    # For string columns, fill with mode or 'Unknown'
                    mode_value = df_clean[col].mode()
                    if len(mode_value) > 0:
                        df_clean[col] = df_clean[col].fillna(mode_value[0])
                    else:
                        df_clean[col] = df_clean[col].fillna('Unknown')
                else:
                    # For other types, forward fill
                    df_clean[col] = df_clean[col].ffill()
        
        return df_clean
    
    def _add_processing_metadata(self, df: pd.DataFrame, source_blob: str) -> pd.DataFrame:
        """
        Add processing metadata columns to DataFrame
        
        Args:
            df: Input DataFrame
            source_blob: Source blob name
            
        Returns:
            DataFrame with metadata columns added
        """
        df_clean = df.copy()
        
        # Add metadata columns
        df_clean['_source_file'] = source_blob
        df_clean['_processed_timestamp'] = datetime.now().isoformat()
        df_clean['_processing_layer'] = 'silver'
        df_clean['_record_id'] = range(1, len(df_clean) + 1)
        
        return df_clean
    
    def _apply_business_transformations(self, df: pd.DataFrame, source_blob: str) -> pd.DataFrame:
        """
        Apply business-specific transformations to prepare data for gold layer
        
        Args:
            df: Input DataFrame
            source_blob: Source blob name
            
        Returns:
            DataFrame with business transformations applied
        """
        df_transformed = df.copy()
        
        # Extract file type from blob name
        file_type = source_blob.split('/')[-1].split('_')[0].lower()
        
        try:
            if file_type == 'orders':
                # Transform orders data for gold layer
                df_transformed = self._transform_orders_data(df_transformed)
            elif file_type == 'performance':
                # Transform performance data for gold layer
                df_transformed = self._transform_performance_data(df_transformed)
            elif file_type == 'fuel':
                # Transform fuel data for gold layer
                df_transformed = self._transform_fuel_data(df_transformed)
            elif file_type == 'fulfillment':
                # Transform fulfillment data for gold layer
                df_transformed = self._transform_fulfillment_data(df_transformed)
            elif file_type == 'inventory':
                # Transform inventory data for gold layer
                df_transformed = self._transform_inventory_data(df_transformed)
            elif file_type == 'maintenance':
                # Transform maintenance data for gold layer
                df_transformed = self._transform_maintenance_data(df_transformed)
            elif file_type == 'order_items':
                # Transform order items data for gold layer
                df_transformed = self._transform_order_items_data(df_transformed)
            elif file_type == 'routes':
                # Transform routes data for gold layer
                df_transformed = self._transform_routes_data(df_transformed)
            elif file_type == 'schedules':
                # Transform schedules data for gold layer
                df_transformed = self._transform_schedules_data(df_transformed)
            elif file_type == 'suppliers':
                # Transform suppliers data for gold layer
                df_transformed = self._transform_suppliers_data(df_transformed)
            elif file_type == 'supply_chain_metrics':
                # Transform supply chain metrics data for gold layer
                df_transformed = self._transform_supply_chain_metrics_data(df_transformed)
            elif file_type == 'telemetry':
                # Transform telemetry data for gold layer
                df_transformed = self._transform_telemetry_data(df_transformed)
            elif file_type == 'vehicles':
                # Transform vehicles data for gold layer
                df_transformed = self._transform_vehicles_data(df_transformed)
            elif file_type == 'warehouses':
                # Transform warehouses data for gold layer
                df_transformed = self._transform_warehouses_data(df_transformed)
            else:
                logger.warning(f"Unknown file type: {file_type}, skipping business transformations")
                
        except Exception as e:
            logger.error(f"Error applying business transformations for {file_type}: {str(e)}")
            # Return original data if transformation fails
            return df
        
        return df_transformed
    
    def _transform_orders_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform orders data for gold layer compatibility"""
        df_transformed = df.copy()
        
        # Map existing columns to gold layer expected columns
        column_mapping = {
            'total_amount_sar': 'total_amount',
            'order_date': 'order_date',
            'customer_id': 'customer_id',
            'order_status': 'order_status',
            'payment_status': 'payment_status',
            'delivery_date': 'delivery_date',
            'priority': 'priority',
            'order_type': 'order_type'
        }
        
        # Rename columns to match gold layer expectations
        for old_col, new_col in column_mapping.items():
            if old_col in df_transformed.columns:
                df_transformed = df_transformed.rename(columns={old_col: new_col})
        
        # Ensure total_amount is numeric
        if 'total_amount' in df_transformed.columns:
            df_transformed['total_amount'] = pd.to_numeric(df_transformed['total_amount'], errors='coerce')
        
        # Ensure order_date is datetime
        if 'order_date' in df_transformed.columns:
            df_transformed['order_date'] = pd.to_datetime(df_transformed['order_date'], errors='coerce')
        
        return df_transformed
    
    def _transform_performance_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform performance data for gold layer compatibility"""
        df_transformed = df.copy()
        
        # Map existing columns to gold layer expected columns
        column_mapping = {
            'total_delivery_time_hours': 'delivery_time',
            'total_distance_km': 'distance_traveled',
            'safety_score': 'efficiency_score',
            'total_fuel_consumed_liters': 'fuel_consumed',
            'date': 'date',
            'vehicle_id': 'vehicle_id',
            'driver_id': 'driver_id',
            'route_id': 'route_id'
        }
        
        # Rename columns to match gold layer expectations
        for old_col, new_col in column_mapping.items():
            if old_col in df_transformed.columns:
                df_transformed = df_transformed.rename(columns={old_col: new_col})
        
        # Ensure numeric columns are properly typed
        numeric_columns = ['delivery_time', 'distance_traveled', 'efficiency_score', 'fuel_consumed']
        for col in numeric_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
        
        # Ensure date is datetime
        if 'date' in df_transformed.columns:
            df_transformed['date'] = pd.to_datetime(df_transformed['date'], errors='coerce')
        
        return df_transformed
    
    def _transform_fuel_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform fuel data for gold layer compatibility"""
        df_transformed = df.copy()
        
        # Map existing columns to gold layer expected columns
        column_mapping = {
            'fuel_quantity_liters': 'fuel_consumed',
            'fuel_efficiency_km_per_liter': 'fuel_efficiency',
            'fuel_date': 'date',
            'vehicle_id': 'vehicle_id',
            'driver_id': 'driver_id',
            'route_id': 'route_id',
            'total_cost_sar': 'fuel_cost'
        }
        
        # Rename columns to match gold layer expectations
        for old_col, new_col in column_mapping.items():
            if old_col in df_transformed.columns:
                df_transformed = df_transformed.rename(columns={old_col: new_col})
        
        # Ensure numeric columns are properly typed
        numeric_columns = ['fuel_consumed', 'fuel_efficiency', 'fuel_cost']
        for col in numeric_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
        
        # Ensure date is datetime
        if 'date' in df_transformed.columns:
            df_transformed['date'] = pd.to_datetime(df_transformed['date'], errors='coerce')
        
        return df_transformed
    
    def _transform_fulfillment_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform fulfillment data for gold layer compatibility"""
        df_transformed = df.copy()
        
        # Map existing columns to gold layer expected columns
        column_mapping = {
            'fulfillment_date': 'date',
            'order_id': 'order_id',
            'fulfillment_status': 'status',
            'warehouse_id': 'warehouse_id'
        }
        
        # Rename columns to match gold layer expectations
        for old_col, new_col in column_mapping.items():
            if old_col in df_transformed.columns:
                df_transformed = df_transformed.rename(columns={old_col: new_col})
        
        # Ensure date is datetime
        if 'date' in df_transformed.columns:
            df_transformed['date'] = pd.to_datetime(df_transformed['date'], errors='coerce')
        
        return df_transformed
    
    def _transform_inventory_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform inventory data for gold layer compatibility"""
        df_transformed = df.copy()
        
        # Map existing columns to gold layer expected columns
        column_mapping = {
            'depot_id': 'warehouse_id',
            'item_code': 'item_id',
            'item_name': 'item_name',
            'quantity_in_stock': 'quantity',
            'unit_cost_sar': 'unit_cost',
            'last_restocked_date': 'date'
        }
        
        # Rename columns to match gold layer expectations
        for old_col, new_col in column_mapping.items():
            if old_col in df_transformed.columns:
                df_transformed = df_transformed.rename(columns={old_col: new_col})
        
        # Ensure numeric columns are properly typed
        numeric_columns = ['quantity', 'unit_cost']
        for col in numeric_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
        
        # Ensure date is datetime
        if 'date' in df_transformed.columns:
            df_transformed['date'] = pd.to_datetime(df_transformed['date'], errors='coerce')
        
        return df_transformed
    
    def _transform_maintenance_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform maintenance data for gold layer compatibility"""
        df_transformed = df.copy()
        
        # Map existing columns to gold layer expected columns
        column_mapping = {
            'maintenance_date': 'date',
            'vehicle_id': 'vehicle_id',
            'cost_sar': 'maintenance_cost',
            'duration_hours': 'duration',
            'maintenance_type': 'type'
        }
        
        # Rename columns to match gold layer expectations
        for old_col, new_col in column_mapping.items():
            if old_col in df_transformed.columns:
                df_transformed = df_transformed.rename(columns={old_col: new_col})
        
        # Ensure numeric columns are properly typed
        numeric_columns = ['maintenance_cost', 'duration']
        for col in numeric_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
        
        # Ensure date is datetime
        if 'date' in df_transformed.columns:
            df_transformed['date'] = pd.to_datetime(df_transformed['date'], errors='coerce')
        
        return df_transformed
    
    def _transform_order_items_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform order items data for gold layer compatibility"""
        df_transformed = df.copy()
        
        # Map existing columns to gold layer expected columns
        column_mapping = {
            'order_id': 'order_id',
            'product_code': 'product_id',
            'product_name': 'product_name',
            'quantity_ordered': 'quantity',
            'unit_price_sar': 'unit_price',
            'total_price_sar': 'total_price'
        }
        
        # Rename columns to match gold layer expectations
        for old_col, new_col in column_mapping.items():
            if old_col in df_transformed.columns:
                df_transformed = df_transformed.rename(columns={old_col: new_col})
        
        # Ensure numeric columns are properly typed
        numeric_columns = ['quantity', 'unit_price', 'total_price']
        for col in numeric_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
        
        return df_transformed
    
    def _transform_routes_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform routes data for gold layer compatibility"""
        df_transformed = df.copy()
        
        # Map existing columns to gold layer expected columns
        column_mapping = {
            'route_id': 'route_id',
            'vehicle_id': 'vehicle_id',
            'driver_id': 'driver_id',
            'distance_km': 'distance',
            'actual_travel_time_hours': 'travel_time',
            'fuel_consumed_liters': 'fuel_consumed',
            'start_time': 'start_time',
            'end_time': 'end_time'
        }
        
        # Rename columns to match gold layer expectations
        for old_col, new_col in column_mapping.items():
            if old_col in df_transformed.columns:
                df_transformed = df_transformed.rename(columns={old_col: new_col})
        
        # Ensure numeric columns are properly typed
        numeric_columns = ['distance', 'travel_time', 'fuel_consumed']
        for col in numeric_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
        
        # Ensure datetime columns are properly typed
        datetime_columns = ['start_time', 'end_time']
        for col in datetime_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')
        
        return df_transformed
    
    def _transform_schedules_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform schedules data for gold layer compatibility"""
        df_transformed = df.copy()
        
        # Map existing columns to gold layer expected columns
        column_mapping = {
            'scheduled_date': 'date',
            'customer_id': 'customer_id',
            'vehicle_id': 'vehicle_id',
            'driver_id': 'driver_id',
            'priority_level': 'priority',
            'status': 'status'
        }
        
        # Rename columns to match gold layer expectations
        for old_col, new_col in column_mapping.items():
            if old_col in df_transformed.columns:
                df_transformed = df_transformed.rename(columns={old_col: new_col})
        
        # Ensure date is datetime
        if 'date' in df_transformed.columns:
            df_transformed['date'] = pd.to_datetime(df_transformed['date'], errors='coerce')
        
        return df_transformed
    
    def _transform_suppliers_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform suppliers data for gold layer compatibility"""
        df_transformed = df.copy()
        
        # Map existing columns to gold layer expected columns
        column_mapping = {
            'supplier_id': 'supplier_id',
            'supplier_name': 'supplier_name',
            'quality_rating': 'quality_score',
            'on_time_delivery_rate': 'delivery_rate',
            'total_orders': 'total_orders',
            'total_value_sar': 'total_value'
        }
        
        # Rename columns to match gold layer expectations
        for old_col, new_col in column_mapping.items():
            if old_col in df_transformed.columns:
                df_transformed = df_transformed.rename(columns={old_col: new_col})
        
        # Ensure numeric columns are properly typed
        numeric_columns = ['quality_score', 'delivery_rate', 'total_orders', 'total_value']
        for col in numeric_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
        
        return df_transformed
    
    def _transform_supply_chain_metrics_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform supply chain metrics data for gold layer compatibility"""
        df_transformed = df.copy()
        
        # Map existing columns to gold layer expected columns
        column_mapping = {
            'date': 'date',
            'warehouse_id': 'warehouse_id',
            'supplier_id': 'supplier_id',
            'inventory_turnover': 'inventory_turnover',
            'stock_accuracy_percent': 'stock_accuracy',
            'order_fulfillment_rate': 'fulfillment_rate',
            'on_time_delivery_rate': 'delivery_rate',
            'customer_satisfaction_score': 'customer_satisfaction'
        }
        
        # Rename columns to match gold layer expectations
        for old_col, new_col in column_mapping.items():
            if old_col in df_transformed.columns:
                df_transformed = df_transformed.rename(columns={old_col: new_col})
        
        # Ensure numeric columns are properly typed
        numeric_columns = ['inventory_turnover', 'stock_accuracy', 'fulfillment_rate', 'delivery_rate', 'customer_satisfaction']
        for col in numeric_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
        
        # Ensure date is datetime
        if 'date' in df_transformed.columns:
            df_transformed['date'] = pd.to_datetime(df_transformed['date'], errors='coerce')
        
        return df_transformed
    
    def _transform_telemetry_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform telemetry data for gold layer compatibility"""
        df_transformed = df.copy()
        
        # Map existing columns to gold layer expected columns
        column_mapping = {
            'timestamp': 'date',
            'vehicle_id': 'vehicle_id',
            'driver_id': 'driver_id',
            'route_id': 'route_id',
            'speed_kmh': 'speed',
            'fuel_level_percent': 'fuel_level',
            'engine_temperature_celsius': 'engine_temp'
        }
        
        # Rename columns to match gold layer expectations
        for old_col, new_col in column_mapping.items():
            if old_col in df_transformed.columns:
                df_transformed = df_transformed.rename(columns={old_col: new_col})
        
        # Ensure numeric columns are properly typed
        numeric_columns = ['speed', 'fuel_level', 'engine_temp']
        for col in numeric_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
        
        # Ensure date is datetime
        if 'date' in df_transformed.columns:
            df_transformed['date'] = pd.to_datetime(df_transformed['date'], errors='coerce')
        
        return df_transformed
    
    def _transform_vehicles_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform vehicles data for gold layer compatibility"""
        df_transformed = df.copy()
        
        # Map existing columns to gold layer expected columns
        column_mapping = {
            'vehicle_id': 'vehicle_id',
            'vehicle_type': 'type',
            'make': 'make',
            'model': 'model',
            'year': 'year',
            'driver_id': 'driver_id',
            'status': 'status',
            'fuel_efficiency': 'fuel_efficiency',
            'mileage': 'mileage'
        }
        
        # Rename columns to match gold layer expectations
        for old_col, new_col in column_mapping.items():
            if old_col in df_transformed.columns:
                df_transformed = df_transformed.rename(columns={old_col: new_col})
        
        # Ensure numeric columns are properly typed
        numeric_columns = ['year', 'fuel_efficiency', 'mileage']
        for col in numeric_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
        
        return df_transformed
    
    def _transform_warehouses_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform warehouses data for gold layer compatibility"""
        df_transformed = df.copy()
        
        # Map existing columns to gold layer expected columns
        column_mapping = {
            'warehouse_id': 'warehouse_id',
            'warehouse_name': 'warehouse_name',
            'city': 'city',
            'country': 'country',
            'capacity_cubic_meters': 'capacity',
            'current_utilization_percent': 'utilization'
        }
        
        # Rename columns to match gold layer expectations
        for old_col, new_col in column_mapping.items():
            if old_col in df_transformed.columns:
                df_transformed = df_transformed.rename(columns={old_col: new_col})
        
        # Ensure numeric columns are properly typed
        numeric_columns = ['capacity', 'utilization']
        for col in numeric_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
        
        return df_transformed
    
    def _transform_data(self, df: pd.DataFrame, source_blob: str) -> pd.DataFrame:
        """
        Apply all transformations to the DataFrame
        
        Args:
            df: Input DataFrame
            source_blob: Source blob name
            
        Returns:
            Transformed DataFrame
        """
        logger.info(f"Starting transformation for {source_blob}")
        
        # Apply transformations in sequence
        df_transformed = df.copy()
        
        # 1. Clean column names
        df_transformed = self._clean_column_names(df_transformed)
        logger.info("[OK] Cleaned column names")
        
        # 2. Standardize data types
        df_transformed = self._standardize_data_types(df_transformed)
        logger.info("[OK] Standardized data types")
        
        # 3. Apply business-specific transformations for gold layer compatibility
        df_transformed = self._apply_business_transformations(df_transformed, source_blob)
        logger.info("[OK] Applied business transformations")
        
        # 4. Remove duplicates
        df_transformed = self._remove_duplicates(df_transformed)
        logger.info("[OK] Removed duplicates")
        
        # 5. Handle missing values
        df_transformed = self._handle_missing_values(df_transformed)
        logger.info("[OK] Handled missing values")
        
        # 6. Add processing metadata
        df_transformed = self._add_processing_metadata(df_transformed, source_blob)
        logger.info("[OK] Added processing metadata")
        
        logger.info(f"Transformation completed: {len(df_transformed)} rows, {len(df_transformed.columns)} columns")
        
        return df_transformed
    
    def _get_silver_blob_name(self, bronze_blob_name: str) -> str:
        """
        Generate silver blob name from bronze blob name
        
        Args:
            bronze_blob_name: Bronze blob name
            
        Returns:
            Silver blob name
        """
        # Extract filename from bronze blob path
        filename = bronze_blob_name.split('/')[-1]
        
        # Remove .csv extension and add _silver suffix
        base_name = filename.replace('.csv', '')
        silver_filename = f"{base_name}_silver.csv"
        
        # Keep the same directory structure
        directory_path = '/'.join(bronze_blob_name.split('/')[:-1])
        if directory_path:
            return f"{directory_path}/{silver_filename}"
        else:
            return silver_filename
    
    def process_bronze_to_silver(self) -> Dict[str, int]:
        """
        Process all files from bronze to silver layer
        
        Returns:
            Dict with processing statistics
        """
        stats = {
            "total_bronze_files": 0,
            "processed_files": 0,
            "skipped_files": 0,
            "failed_files": 0,
            "total_rows_processed": 0
        }
        
        try:
            # List all blobs in bronze container and filter to CSV files only
            all_bronze_blobs = list(self.bronze_container_client.list_blobs())
            bronze_blobs = [b for b in all_bronze_blobs if b.name.lower().endswith('.csv')]
            stats["total_bronze_files"] = len(bronze_blobs)
            
            logger.info(f"Found {len(bronze_blobs)} files in bronze container")
            
            for blob in bronze_blobs:
                blob_name = blob.name
                
                # Skip if already processed
                if blob_name in self.processed_files:
                    stats["skipped_files"] += 1
                    continue
                
                try:
                    # Download from bronze
                    df = self._download_blob_to_dataframe(blob_name)
                    if df is None:
                        stats["failed_files"] += 1
                        continue
                    
                    # Transform data
                    df_transformed = self._transform_data(df, blob_name)
                    
                    # Generate silver blob name
                    silver_blob_name = self._get_silver_blob_name(blob_name)
                    
                    # Upload to silver
                    if self._upload_dataframe_to_blob(df_transformed, silver_blob_name):
                        # Update metadata
                        self.processed_files[blob_name] = {
                            "processed_timestamp": datetime.now().isoformat(),
                            "silver_blob_name": silver_blob_name,
                            "row_count": len(df_transformed),
                            "column_count": len(df_transformed.columns)
                        }
                        
                        stats["processed_files"] += 1
                        stats["total_rows_processed"] += len(df_transformed)
                        
                        logger.info(f"Successfully processed: {blob_name} -> {silver_blob_name}")
                    else:
                        stats["failed_files"] += 1
                        
                except Exception as e:
                    logger.error(f"Error processing {blob_name}: {str(e)}")
                    stats["failed_files"] += 1
            
            # Save updated metadata
            self._save_metadata()
            
            logger.info(f"Processing completed. Stats: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error in bronze to silver processing: {str(e)}")
            return stats
    
    def get_processing_status(self) -> Dict:
        """
        Get current processing status and statistics
        
        Returns:
            Dict with status information
        """
        try:
            # List blobs in both containers
            bronze_blobs = list(self.bronze_container_client.list_blobs())
            silver_blobs = list(self.silver_container_client.list_blobs())
            
            status = {
                "bronze_container": self.bronze_container,
                "silver_container": self.silver_container,
                "bronze_files": len(bronze_blobs),
                "silver_files": len(silver_blobs),
                "processed_files": len(self.processed_files),
                "last_processing": datetime.now().isoformat(),
                "recent_silver_files": []
            }
            
            # Get recent silver blobs (last 10)
            recent_silver = sorted(silver_blobs, key=lambda x: x.last_modified, reverse=True)[:10]
            for blob in recent_silver:
                status["recent_silver_files"].append({
                    "name": blob.name,
                    "size": blob.size,
                    "last_modified": blob.last_modified.isoformat(),
                    "metadata": blob.metadata
                })
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting processing status: {str(e)}")
            return {"error": str(e)}

def main():
    """Main function to run the silver layer processor"""
    
    # Azure Storage connection string
    CONNECTION_STRING = (
        "BlobEndpoint=https://adls4aldress.blob.core.windows.net/;"
        "QueueEndpoint=https://adls4aldress.queue.core.windows.net/;"
        "FileEndpoint=https://adls4aldress.file.core.windows.net/;"
        "TableEndpoint=https://adls4aldress.table.core.windows.net/;"
        "SharedAccessSignature=sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupiytfx&"
        "se=2025-10-06T12:52:45Z&st=2025-10-05T04:37:45Z&spr=https&"
        "sig=EHAifBcmoHCm2SsLy6IVDRbe9RC0pWInKG4fBo8QoYw%3D"
    )
    
    try:
        # Initialize processor
        processor = SilverLayerProcessor(
            connection_string=CONNECTION_STRING,
            bronze_container="bronze",
            silver_container="silver"
        )
        
        # Process bronze to silver
        logger.info("Starting bronze to silver processing...")
        stats = processor.process_bronze_to_silver()
        
        # Print results
        print("\n" + "="*50)
        print("SILVER LAYER PROCESSING SUMMARY")
        print("="*50)
        print(f"Total bronze files: {stats['total_bronze_files']}")
        print(f"Processed files: {stats['processed_files']}")
        print(f"Skipped files: {stats['skipped_files']}")
        print(f"Failed files: {stats['failed_files']}")
        print(f"Total rows processed: {stats['total_rows_processed']}")
        print("="*50)
        
        # Get processing status
        status = processor.get_processing_status()
        print(f"\nBronze container: {status.get('bronze_container', 'N/A')}")
        print(f"Silver container: {status.get('silver_container', 'N/A')}")
        print(f"Bronze files: {status.get('bronze_files', 0)}")
        print(f"Silver files: {status.get('silver_files', 0)}")
        print(f"Processed files: {status.get('processed_files', 0)}")
        
        logger.info("Silver layer processing completed successfully.")
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
