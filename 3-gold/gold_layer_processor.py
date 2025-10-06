"""
Gold Layer Processor for Azure Data Lake Storage Gen2
Transforms Silver layer data into analytics-ready dimensional models with facts, dimensions, and KPIs
"""

import os
import time
import logging
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
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
        logging.FileHandler('gold_layer_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GoldLayerProcessor:
    """
    Gold Layer Processor for Azure Data Lake Storage Gen2
    Creates analytics-ready dimensional models from Silver layer data
    """
    
    def __init__(self, connection_string: str, silver_container: str = "silver", gold_container: str = "gold"):
        """
        Initialize the Gold Layer Processor
        
        Args:
            connection_string: Azure Storage connection string
            silver_container: Name of the silver container
            gold_container: Name of the gold container
        """
        self.connection_string = connection_string
        self.silver_container = silver_container
        self.gold_container = gold_container
        self.blob_service_client = None
        self.silver_container_client = None
        self.gold_container_client = None
        self.processed_files = {}  # Track processed files
        self.metadata_file = "gold_layer_metadata.json"
        
        # Data storage for dimensional modeling
        self.dimensions = {}
        self.facts = {}
        self.kpis = {}
        
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
            
            # Create silver container client
            self.silver_container_client = self.blob_service_client.get_container_client(
                self.silver_container
            )
            
            # Create gold container if it doesn't exist
            try:
                self.gold_container_client = self.blob_service_client.create_container(
                    self.gold_container
                )
                logger.info(f"Created new gold container: {self.gold_container}")
            except ResourceExistsError:
                self.gold_container_client = self.blob_service_client.get_container_client(
                    self.gold_container
                )
                logger.info(f"Using existing gold container: {self.gold_container}")
                
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
        Download a blob from silver container and convert to DataFrame
        
        Args:
            blob_name: Name of the blob to download
            
        Returns:
            DataFrame or None if failed
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.silver_container,
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
        Upload a DataFrame to gold container as CSV
        
        Args:
            df: DataFrame to upload
            blob_name: Name of the blob
            metadata: Optional metadata to attach
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.gold_container,
                blob=blob_name
            )
            
            # Convert DataFrame to CSV
            csv_data = df.to_csv(index=False)
            
            # Upload with metadata
            upload_metadata = {
                "source_silver_blob": blob_name,
                "processing_timestamp": datetime.now().isoformat(),
                "row_count": str(len(df)),
                "column_count": str(len(df.columns)),
                "data_types": str(df.dtypes.to_dict()),
                "layer": "gold"
            }
            
            if metadata:
                upload_metadata.update(metadata)
            
            blob_client.upload_blob(
                csv_data,
                overwrite=True,
                metadata=upload_metadata
            )
            
            logger.info(f"Uploaded to gold: {blob_name} ({len(df)} rows)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload {blob_name}: {str(e)}")
            return False
    
    def _create_dimension_tables(self) -> Dict[str, pd.DataFrame]:
        """
        Create dimension tables from silver data
        
        Returns:
            Dict of dimension tables
        """
        dimensions = {}
        
        try:
            # Get all silver blobs
            silver_blobs = list(self.silver_container_client.list_blobs())
            
            # Collect data for dimensions
            customer_data = []
            product_data = []
            geography_data = []
            time_data = []
            vehicle_data = []
            supplier_data = []
            warehouse_data = []
            
            for blob in silver_blobs:
                if blob.name.endswith('_silver.csv'):
                    df = self._download_blob_to_dataframe(blob.name)
                    if df is None:
                        continue
                    
                    # Extract dimension data based on file type
                    if 'orders' in blob.name or 'order_items' in blob.name:
                        # Customer dimension
                        if 'customer' in df.columns:
                            customer_cols = ['customer_id', 'customer_name', 'customer_email', 'customer_phone', 'customer_type']
                            available_cols = [col for col in customer_cols if col in df.columns]
                            if available_cols:
                                customer_data.append(df[available_cols].drop_duplicates())
                        
                        # Product dimension
                        if 'product' in df.columns:
                            product_cols = ['product_id', 'product_name', 'product_category', 'product_price', 'product_weight']
                            available_cols = [col for col in product_cols if col in df.columns]
                            if available_cols:
                                product_data.append(df[available_cols].drop_duplicates())
                    
                    elif 'vehicles' in blob.name:
                        # Vehicle dimension
                        vehicle_cols = ['vehicle_id', 'vehicle_type', 'make', 'model', 'year', 'capacity', 'fuel_type']
                        available_cols = [col for col in vehicle_cols if col in df.columns]
                        if available_cols:
                            vehicle_data.append(df[available_cols].drop_duplicates())
                    
                    elif 'suppliers' in blob.name:
                        # Supplier dimension
                        supplier_cols = ['supplier_id', 'supplier_name', 'contact_person', 'email', 'phone', 'rating']
                        available_cols = [col for col in supplier_cols if col in df.columns]
                        if available_cols:
                            supplier_data.append(df[available_cols].drop_duplicates())
                    
                    elif 'warehouses' in blob.name:
                        # Warehouse dimension
                        warehouse_cols = ['warehouse_id', 'warehouse_name', 'location', 'city', 'country', 'capacity']
                        available_cols = [col for col in warehouse_cols if col in df.columns]
                        if available_cols:
                            warehouse_data.append(df[available_cols].drop_duplicates())
                    
                    # Geography dimension (from any file with location data)
                    if any(col in df.columns for col in ['city', 'country', 'latitude', 'longitude']):
                        geo_cols = ['city', 'country', 'latitude', 'longitude', 'region']
                        available_cols = [col for col in geo_cols if col in df.columns]
                        if available_cols:
                            geography_data.append(df[available_cols].drop_duplicates())
            
            # Create dimension tables
            if customer_data:
                dimensions['dim_customer'] = pd.concat(customer_data, ignore_index=True).drop_duplicates()
                dimensions['dim_customer']['customer_key'] = range(1, len(dimensions['dim_customer']) + 1)
                logger.info(f"Created dim_customer: {len(dimensions['dim_customer'])} records")
            
            if product_data:
                dimensions['dim_product'] = pd.concat(product_data, ignore_index=True).drop_duplicates()
                dimensions['dim_product']['product_key'] = range(1, len(dimensions['dim_product']) + 1)
                logger.info(f"Created dim_product: {len(dimensions['dim_product'])} records")
            
            if vehicle_data:
                dimensions['dim_vehicle'] = pd.concat(vehicle_data, ignore_index=True).drop_duplicates()
                dimensions['dim_vehicle']['vehicle_key'] = range(1, len(dimensions['dim_vehicle']) + 1)
                logger.info(f"Created dim_vehicle: {len(dimensions['dim_vehicle'])} records")
            
            if supplier_data:
                dimensions['dim_supplier'] = pd.concat(supplier_data, ignore_index=True).drop_duplicates()
                dimensions['dim_supplier']['supplier_key'] = range(1, len(dimensions['dim_supplier']) + 1)
                logger.info(f"Created dim_supplier: {len(dimensions['dim_supplier'])} records")
            
            if warehouse_data:
                dimensions['dim_warehouse'] = pd.concat(warehouse_data, ignore_index=True).drop_duplicates()
                dimensions['dim_warehouse']['warehouse_key'] = range(1, len(dimensions['dim_warehouse']) + 1)
                logger.info(f"Created dim_warehouse: {len(dimensions['dim_warehouse'])} records")
            
            if geography_data:
                dimensions['dim_geography'] = pd.concat(geography_data, ignore_index=True).drop_duplicates()
                dimensions['dim_geography']['geography_key'] = range(1, len(dimensions['dim_geography']) + 1)
                logger.info(f"Created dim_geography: {len(dimensions['dim_geography'])} records")
            
            # Create time dimension
            dimensions['dim_time'] = self._create_time_dimension()
            logger.info(f"Created dim_time: {len(dimensions['dim_time'])} records")
            
            self.dimensions = dimensions
            return dimensions
            
        except Exception as e:
            logger.error(f"Error creating dimension tables: {str(e)}")
            return {}
    
    def _create_time_dimension(self) -> pd.DataFrame:
        """
        Create time dimension table
        
        Returns:
            Time dimension DataFrame
        """
        # Create date range for the last 2 years
        end_date = datetime.now()
        start_date = end_date - timedelta(days=730)
        
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        time_dim = pd.DataFrame({
            'date_key': dates.strftime('%Y%m%d').astype(int),
            'date': dates,
            'year': dates.year,
            'quarter': dates.quarter,
            'month': dates.month,
            'month_name': dates.strftime('%B'),
            'week': dates.isocalendar().week,
            'day_of_year': dates.dayofyear,
            'day_of_month': dates.day,
            'day_of_week': dates.dayofweek,
            'day_name': dates.strftime('%A'),
            'is_weekend': dates.dayofweek.isin([5, 6]),
            'is_holiday': False,  # Could be enhanced with holiday calendar
            'fiscal_year': dates.year,
            'fiscal_quarter': ((dates.month - 1) // 3) + 1
        })
        
        return time_dim
    
    def _create_fact_tables(self) -> Dict[str, pd.DataFrame]:
        """
        Create fact tables from silver data
        
        Returns:
            Dict of fact tables
        """
        facts = {}
        
        try:
            # Get all silver blobs
            silver_blobs = list(self.silver_container_client.list_blobs())
            
            # Collect data for facts
            orders_data = []
            performance_data = []
            fuel_data = []
            inventory_data = []
            
            for blob in silver_blobs:
                if blob.name.endswith('_silver.csv'):
                    df = self._download_blob_to_dataframe(blob.name)
                    if df is None:
                        continue
                    
                    # Create fact tables based on file type
                    if 'orders' in blob.name:
                        # Orders fact
                        fact_cols = ['order_id', 'customer_id', 'product_id', 'order_date', 'quantity', 'unit_price', 'total_amount']
                        available_cols = [col for col in fact_cols if col in df.columns]
                        if available_cols:
                            orders_data.append(df[available_cols])
                    
                    elif 'performance' in blob.name:
                        # Performance fact
                        fact_cols = ['vehicle_id', 'date', 'distance_traveled', 'fuel_consumed', 'delivery_time', 'efficiency_score']
                        available_cols = [col for col in fact_cols if col in df.columns]
                        if available_cols:
                            performance_data.append(df[available_cols])
                    
                    elif 'fuel' in blob.name:
                        # Fuel consumption fact
                        fact_cols = ['vehicle_id', 'date', 'fuel_type', 'fuel_consumed', 'cost', 'efficiency']
                        available_cols = [col for col in fact_cols if col in df.columns]
                        if available_cols:
                            fuel_data.append(df[available_cols])
                    
                    elif 'inventory' in blob.name:
                        # Inventory fact
                        fact_cols = ['product_id', 'warehouse_id', 'date', 'quantity_on_hand', 'quantity_ordered', 'quantity_sold']
                        available_cols = [col for col in fact_cols if col in df.columns]
                        if available_cols:
                            inventory_data.append(df[available_cols])
            
            # Create fact tables
            if orders_data:
                facts['fact_orders'] = pd.concat(orders_data, ignore_index=True)
                # Add calculated fields
                if 'quantity' in facts['fact_orders'].columns and 'unit_price' in facts['fact_orders'].columns:
                    facts['fact_orders']['total_amount'] = facts['fact_orders']['quantity'] * facts['fact_orders']['unit_price']
                logger.info(f"Created fact_orders: {len(facts['fact_orders'])} records")
            
            if performance_data:
                facts['fact_performance'] = pd.concat(performance_data, ignore_index=True)
                logger.info(f"Created fact_performance: {len(facts['fact_performance'])} records")
            
            if fuel_data:
                facts['fact_fuel_consumption'] = pd.concat(fuel_data, ignore_index=True)
                logger.info(f"Created fact_fuel_consumption: {len(facts['fact_fuel_consumption'])} records")
            
            if inventory_data:
                facts['fact_inventory'] = pd.concat(inventory_data, ignore_index=True)
                logger.info(f"Created fact_inventory: {len(facts['fact_inventory'])} records")
            
            self.facts = facts
            return facts
            
        except Exception as e:
            logger.error(f"Error creating fact tables: {str(e)}")
            return {}
    
    def _calculate_kpis(self) -> Dict[str, pd.DataFrame]:
        """
        Calculate KPIs and business metrics
        
        Returns:
            Dict of KPI tables
        """
        kpis = {}
        
        try:
            # Revenue KPIs
            if 'fact_orders' in self.facts:
                orders_fact = self.facts['fact_orders'].copy()

                # Ensure total_amount exists
                if 'total_amount' not in orders_fact.columns:
                    if 'quantity' in orders_fact.columns and 'unit_price' in orders_fact.columns:
                        orders_fact['total_amount'] = orders_fact['quantity'] * orders_fact['unit_price']
                    else:
                        # If cannot compute, create zero column to avoid failure
                        orders_fact['total_amount'] = 0.0

                # Ensure order_date is datetime
                if 'order_date' in orders_fact.columns:
                    orders_fact['order_date'] = pd.to_datetime(orders_fact['order_date'], errors='coerce')
                else:
                    # If missing, skip revenue KPIs
                    orders_fact['order_date'] = pd.NaT

                # Drop rows without valid date
                orders_fact = orders_fact.dropna(subset=['order_date'])

                # Daily revenue
                if not orders_fact.empty:
                    daily_revenue = orders_fact.groupby(orders_fact['order_date'].dt.date).agg({
                        'total_amount': 'sum',
                        'order_id': 'count'
                    }).reset_index()
                    daily_revenue.columns = ['date', 'daily_revenue', 'order_count']
                    daily_revenue['avg_order_value'] = daily_revenue.apply(
                        lambda r: (r['daily_revenue'] / r['order_count']) if r['order_count'] else 0.0, axis=1
                    )
                    kpis['daily_revenue'] = daily_revenue

                    # Monthly revenue
                    orders_fact['year_month'] = orders_fact['order_date'].dt.to_period('M')
                    monthly_revenue = orders_fact.groupby('year_month').agg({
                        'total_amount': 'sum',
                        'order_id': 'count'
                    }).reset_index()
                    monthly_revenue.columns = ['year_month', 'monthly_revenue', 'order_count']
                    monthly_revenue['avg_order_value'] = monthly_revenue.apply(
                        lambda r: (r['monthly_revenue'] / r['order_count']) if r['order_count'] else 0.0, axis=1
                    )
                    kpis['monthly_revenue'] = monthly_revenue
            
            # Performance KPIs
            if 'fact_performance' in self.facts:
                perf_fact = self.facts['fact_performance']
                
                # Vehicle performance metrics
                vehicle_performance = perf_fact.groupby('vehicle_id').agg({
                    'distance_traveled': 'sum',
                    'fuel_consumed': 'sum',
                    'delivery_time': 'mean',
                    'efficiency_score': 'mean'
                }).reset_index()
                vehicle_performance['fuel_efficiency'] = vehicle_performance['distance_traveled'] / vehicle_performance['fuel_consumed']
                kpis['vehicle_performance'] = vehicle_performance
            
            # Inventory KPIs
            if 'fact_inventory' in self.facts:
                inv_fact = self.facts['fact_inventory']
                
                # Inventory turnover
                inventory_turnover = inv_fact.groupby('product_id').agg({
                    'quantity_sold': 'sum',
                    'quantity_on_hand': 'mean'
                }).reset_index()
                inventory_turnover['turnover_ratio'] = inventory_turnover['quantity_sold'] / inventory_turnover['quantity_on_hand']
                kpis['inventory_turnover'] = inventory_turnover
            
            # Supply chain KPIs
            supply_chain_kpis = self._calculate_supply_chain_kpis()
            kpis.update(supply_chain_kpis)
            
            self.kpis = kpis
            return kpis
            
        except Exception as e:
            logger.error(f"Error calculating KPIs: {str(e)}")
            return {}
    
    def _calculate_supply_chain_kpis(self) -> Dict[str, pd.DataFrame]:
        """
        Calculate supply chain specific KPIs
        
        Returns:
            Dict of supply chain KPI tables
        """
        sc_kpis = {}
        
        try:
            # Get all silver data for comprehensive analysis
            silver_blobs = list(self.silver_container_client.list_blobs())
            
            # Collect all data
            all_data = []
            for blob in silver_blobs:
                if blob.name.endswith('_silver.csv'):
                    df = self._download_blob_to_dataframe(blob.name)
                    if df is not None:
                        df['source_table'] = blob.name.replace('_silver.csv', '')
                        all_data.append(df)
            
            if all_data:
                combined_data = pd.concat(all_data, ignore_index=True)
                
                # On-time delivery rate
                if 'delivery_date' in combined_data.columns and 'estimated_delivery' in combined_data.columns:
                    combined_data['on_time'] = combined_data['delivery_date'] <= combined_data['estimated_delivery']
                    on_time_rate = combined_data.groupby('source_table')['on_time'].mean().reset_index()
                    on_time_rate.columns = ['table_name', 'on_time_delivery_rate']
                    sc_kpis['on_time_delivery'] = on_time_rate
                
                # Order fulfillment rate
                if 'order_status' in combined_data.columns:
                    fulfillment_rate = combined_data.groupby('source_table')['order_status'].apply(
                        lambda x: (x == 'fulfilled').mean()
                    ).reset_index()
                    fulfillment_rate.columns = ['table_name', 'fulfillment_rate']
                    sc_kpis['fulfillment_rate'] = fulfillment_rate
                
                # Cost efficiency metrics
                if 'cost' in combined_data.columns and 'revenue' in combined_data.columns:
                    cost_efficiency = combined_data.groupby('source_table').agg({
                        'cost': 'sum',
                        'revenue': 'sum'
                    }).reset_index()
                    cost_efficiency['cost_efficiency_ratio'] = cost_efficiency['revenue'] / cost_efficiency['cost']
                    sc_kpis['cost_efficiency'] = cost_efficiency
            
            return sc_kpis
            
        except Exception as e:
            logger.error(f"Error calculating supply chain KPIs: {str(e)}")
            return {}
    
    def _create_analytics_views(self) -> Dict[str, pd.DataFrame]:
        """
        Create analytics views for Power BI and Databricks
        
        Returns:
            Dict of analytics views
        """
        views = {}
        
        try:
            # Revenue analytics view
            if 'daily_revenue' in self.kpis and 'dim_time' in self.dimensions:
                revenue_view = self.kpis['daily_revenue'].copy()
                revenue_view['date'] = pd.to_datetime(revenue_view['date'])
                revenue_view = revenue_view.merge(
                    self.dimensions['dim_time'][['date', 'year', 'quarter', 'month', 'month_name', 'day_name']],
                    on='date',
                    how='left'
                )
                views['revenue_analytics'] = revenue_view
            
            # Performance analytics view
            if 'vehicle_performance' in self.kpis and 'dim_vehicle' in self.dimensions:
                perf_view = self.kpis['vehicle_performance'].copy()
                perf_view = perf_view.merge(
                    self.dimensions['dim_vehicle'][['vehicle_id', 'vehicle_type', 'make', 'model']],
                    on='vehicle_id',
                    how='left'
                )
                views['performance_analytics'] = perf_view
            
            # Inventory analytics view
            if 'inventory_turnover' in self.kpis and 'dim_product' in self.dimensions:
                inv_view = self.kpis['inventory_turnover'].copy()
                inv_view = inv_view.merge(
                    self.dimensions['dim_product'][['product_id', 'product_name', 'product_category']],
                    on='product_id',
                    how='left'
                )
                views['inventory_analytics'] = inv_view
            
            return views
            
        except Exception as e:
            logger.error(f"Error creating analytics views: {str(e)}")
            return {}
    
    def process_silver_to_gold(self) -> Dict[str, int]:
        """
        Process all data from silver to gold layer
        
        Returns:
            Dict with processing statistics
        """
        stats = {
            "total_silver_files": 0,
            "dimensions_created": 0,
            "facts_created": 0,
            "kpis_calculated": 0,
            "views_created": 0,
            "failed_operations": 0
        }
        
        try:
            # Get silver files count
            silver_blobs = list(self.silver_container_client.list_blobs())
            stats["total_silver_files"] = len(silver_blobs)
            
            logger.info("Starting Gold Layer processing...")
            
            # Step 1: Create dimension tables
            logger.info("Creating dimension tables...")
            dimensions = self._create_dimension_tables()
            stats["dimensions_created"] = len(dimensions)
            
            # Upload dimensions
            for dim_name, dim_df in dimensions.items():
                blob_name = f"dimensions/{dim_name}.csv"
                if self._upload_dataframe_to_blob(dim_df, blob_name, {"table_type": "dimension"}):
                    logger.info(f"Uploaded {dim_name}: {len(dim_df)} records")
                else:
                    stats["failed_operations"] += 1
            
            # Step 2: Create fact tables
            logger.info("Creating fact tables...")
            facts = self._create_fact_tables()
            stats["facts_created"] = len(facts)
            
            # Upload facts
            for fact_name, fact_df in facts.items():
                blob_name = f"facts/{fact_name}.csv"
                if self._upload_dataframe_to_blob(fact_df, blob_name, {"table_type": "fact"}):
                    logger.info(f"Uploaded {fact_name}: {len(fact_df)} records")
                else:
                    stats["failed_operations"] += 1
            
            # Step 3: Calculate KPIs
            logger.info("Calculating KPIs...")
            kpis = self._calculate_kpis()
            stats["kpis_calculated"] = len(kpis)
            
            # Upload KPIs
            for kpi_name, kpi_df in kpis.items():
                blob_name = f"kpis/{kpi_name}.csv"
                if self._upload_dataframe_to_blob(kpi_df, blob_name, {"table_type": "kpi"}):
                    logger.info(f"Uploaded {kpi_name}: {len(kpi_df)} records")
                else:
                    stats["failed_operations"] += 1
            
            # Step 4: Create analytics views
            logger.info("Creating analytics views...")
            views = self._create_analytics_views()
            stats["views_created"] = len(views)
            
            # Upload views
            for view_name, view_df in views.items():
                blob_name = f"analytics/{view_name}.csv"
                if self._upload_dataframe_to_blob(view_df, blob_name, {"table_type": "analytics_view"}):
                    logger.info(f"Uploaded {view_name}: {len(view_df)} records")
                else:
                    stats["failed_operations"] += 1
            
            # Save metadata
            self._save_metadata()
            
            logger.info(f"Gold Layer processing completed. Stats: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error in silver to gold processing: {str(e)}")
            return stats
    
    def get_processing_status(self) -> Dict:
        """
        Get current processing status and statistics
        
        Returns:
            Dict with status information
        """
        try:
            # List blobs in both containers
            silver_blobs = list(self.silver_container_client.list_blobs())
            gold_blobs = list(self.gold_container_client.list_blobs())
            
            # Categorize gold blobs
            dimensions = [b for b in gold_blobs if 'dimensions/' in b.name]
            facts = [b for b in gold_blobs if 'facts/' in b.name]
            kpis = [b for b in gold_blobs if 'kpis/' in b.name]
            analytics = [b for b in gold_blobs if 'analytics/' in b.name]
            
            status = {
                "silver_container": self.silver_container,
                "gold_container": self.gold_container,
                "silver_files": len(silver_blobs),
                "gold_files": len(gold_blobs),
                "dimensions": len(dimensions),
                "facts": len(facts),
                "kpis": len(kpis),
                "analytics_views": len(analytics),
                "last_processing": datetime.now().isoformat(),
                "gold_structure": {
                    "dimensions": [b.name for b in dimensions],
                    "facts": [b.name for b in facts],
                    "kpis": [b.name for b in kpis],
                    "analytics": [b.name for b in analytics]
                }
            }
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting processing status: {str(e)}")
            return {"error": str(e)}

def main():
    """Main function to run the gold layer processor"""
    
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
        processor = GoldLayerProcessor(
            connection_string=CONNECTION_STRING,
            silver_container="silver",
            gold_container="gold"
        )
        
        # Process silver to gold
        logger.info("Starting silver to gold processing...")
        stats = processor.process_silver_to_gold()
        
        # Print results
        print("\n" + "="*50)
        print("GOLD LAYER PROCESSING SUMMARY")
        print("="*50)
        print(f"Total silver files: {stats['total_silver_files']}")
        print(f"Dimensions created: {stats['dimensions_created']}")
        print(f"Facts created: {stats['facts_created']}")
        print(f"KPIs calculated: {stats['kpis_calculated']}")
        print(f"Analytics views created: {stats['views_created']}")
        print(f"Failed operations: {stats['failed_operations']}")
        print("="*50)
        
        # Get processing status
        status = processor.get_processing_status()
        print(f"\nSilver container: {status.get('silver_container', 'N/A')}")
        print(f"Gold container: {status.get('gold_container', 'N/A')}")
        print(f"Silver files: {status.get('silver_files', 0)}")
        print(f"Gold files: {status.get('gold_files', 0)}")
        print(f"Dimensions: {status.get('dimensions', 0)}")
        print(f"Facts: {status.get('facts', 0)}")
        print(f"KPIs: {status.get('kpis', 0)}")
        print(f"Analytics views: {status.get('analytics_views', 0)}")
        
        logger.info("Gold layer processing completed successfully.")
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
