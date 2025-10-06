"""
Azure Data Lake Storage Connector for Gold Layer Data
Handles connection and data retrieval from Azure Storage
"""

import pandas as pd
import json
import io
from typing import Dict, List, Optional, Any
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError, AzureError
import streamlit as st
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AzureDataConnector:
    """
    Azure Data Lake Storage connector for retrieving gold layer data
    """
    
    def __init__(self, config_path: str = "../3-gold/gold_config.json"):
        """
        Initialize the Azure Data Connector
        
        Args:
            config_path: Path to the gold layer configuration file
        """
        self.config = self._load_config(config_path)
        self.blob_service_client = None
        self.gold_container_client = None
        self._initialize_connection()
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {str(e)}")
            return {}
    
    def _initialize_connection(self):
        """Initialize Azure Storage connection"""
        try:
            connection_string = self.config.get('azure_storage', {}).get('connection_string')
            if not connection_string:
                raise ValueError("Connection string not found in config")
            
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            gold_container = self.config.get('azure_storage', {}).get('gold_container', 'gold')
            self.gold_container_client = self.blob_service_client.get_container_client(gold_container)
            
            logger.info("Azure Storage connection initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Azure connection: {str(e)}")
            raise
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def get_dimensions(_self) -> Dict[str, pd.DataFrame]:
        """
        Retrieve all dimension tables from gold layer
        
        Returns:
            Dictionary of dimension DataFrames
        """
        dimensions = {}
        
        try:
            # List all blobs in dimensions folder
            dimension_blobs = _self.gold_container_client.list_blobs(name_starts_with="dimensions/")
            
            for blob in dimension_blobs:
                if blob.name.endswith('.csv'):
                    # Extract dimension name from blob path
                    dim_name = blob.name.split('/')[-1].replace('.csv', '')
                    
                    # Download and convert to DataFrame
                    blob_client = _self.gold_container_client.get_blob_client(blob.name)
                    blob_data = blob_client.download_blob().readall()
                    df = pd.read_csv(io.BytesIO(blob_data))
                    
                    dimensions[dim_name] = df
                    logger.info(f"Loaded dimension {dim_name}: {len(df)} rows")
            
        except Exception as e:
            logger.error(f"Error loading dimensions: {str(e)}")
            st.error(f"Error loading dimensions: {str(e)}")
        
        return dimensions
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def get_facts(_self) -> Dict[str, pd.DataFrame]:
        """
        Retrieve all fact tables from gold layer
        
        Returns:
            Dictionary of fact DataFrames
        """
        facts = {}
        
        try:
            # List all blobs in facts folder
            fact_blobs = _self.gold_container_client.list_blobs(name_starts_with="facts/")
            
            for blob in fact_blobs:
                if blob.name.endswith('.csv'):
                    # Extract fact name from blob path
                    fact_name = blob.name.split('/')[-1].replace('.csv', '')
                    
                    # Download and convert to DataFrame
                    blob_client = _self.gold_container_client.get_blob_client(blob.name)
                    blob_data = blob_client.download_blob().readall()
                    df = pd.read_csv(io.BytesIO(blob_data))
                    
                    facts[fact_name] = df
                    logger.info(f"Loaded fact {fact_name}: {len(df)} rows")
            
        except Exception as e:
            logger.error(f"Error loading facts: {str(e)}")
            st.error(f"Error loading facts: {str(e)}")
        
        return facts
    
    @st.cache_data(ttl=300)
    def get_kpis(_self) -> Dict[str, pd.DataFrame]:
        """
        Retrieve KPI data from gold layer
        
        Returns:
            Dictionary of KPI DataFrames
        """
        kpis = {}
        
        try:
            # List all blobs in kpis folder
            kpi_blobs = _self.gold_container_client.list_blobs(name_starts_with="kpis/")
            
            for blob in kpi_blobs:
                if blob.name.endswith('.csv'):
                    # Extract KPI name from blob path
                    kpi_name = blob.name.split('/')[-1].replace('.csv', '')
                    
                    # Download and convert to DataFrame
                    blob_client = _self.gold_container_client.get_blob_client(blob.name)
                    blob_data = blob_client.download_blob().readall()
                    df = pd.read_csv(io.BytesIO(blob_data))
                    
                    kpis[kpi_name] = df
                    logger.info(f"Loaded KPI {kpi_name}: {len(df)} rows")
            
        except Exception as e:
            logger.error(f"Error loading KPIs: {str(e)}")
            st.warning(f"KPI data not available: {str(e)}")
        
        return kpis
    
    def get_analytics_data(self) -> Dict[str, Any]:
        """
        Get comprehensive analytics data including dimensions, facts, and KPIs
        
        Returns:
            Dictionary containing all analytics data
        """
        return {
            'dimensions': self.get_dimensions(),
            'facts': self.get_facts(),
            'kpis': self.get_kpis()
        }
    
    def get_data_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics of all available data
        
        Returns:
            Dictionary with data summary
        """
        data = self.get_analytics_data()
        summary = {
            'dimensions': {},
            'facts': {},
            'kpis': {}
        }
        
        # Summarize dimensions
        for name, df in data['dimensions'].items():
            summary['dimensions'][name] = {
                'rows': len(df),
                'columns': len(df.columns),
                'columns_list': list(df.columns)
            }
        
        # Summarize facts
        for name, df in data['facts'].items():
            summary['facts'][name] = {
                'rows': len(df),
                'columns': len(df.columns),
                'columns_list': list(df.columns)
            }
        
        # Summarize KPIs
        for name, df in data['kpis'].items():
            summary['kpis'][name] = {
                'rows': len(df),
                'columns': len(df.columns),
                'columns_list': list(df.columns)
            }
        
        return summary

# Global connector instance
@st.cache_resource
def get_data_connector():
    """Get cached data connector instance"""
    return AzureDataConnector()
