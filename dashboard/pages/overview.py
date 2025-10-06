"""
Overview Dashboard Page
Main dashboard with key metrics and overview charts
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_connector import get_data_connector
from utils import DataProcessor, ChartGenerator, KPICalculator, create_metric_card, create_info_box

def main():
    """Main overview dashboard function"""
    st.set_page_config(
        page_title="Supply Chain Analytics - Overview",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 Supply Chain Analytics Dashboard")
    st.markdown("### Overview & Key Performance Indicators")
    
    # Initialize data connector
    try:
        connector = get_data_connector()
        data = connector.get_analytics_data()
    except Exception as e:
        st.error(f"Failed to connect to data source: {str(e)}")
        return
    
    # Data summary
    summary = connector.get_data_summary()
    
    # Sidebar filters
    st.sidebar.header("📅 Filters")
    
    # Date range filter
    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(datetime.now() - timedelta(days=30), datetime.now()),
        min_value=datetime.now() - timedelta(days=365),
        max_value=datetime.now()
    )
    
    # Data source selection
    st.sidebar.header("📊 Data Sources")
    show_dimensions = st.sidebar.checkbox("Show Dimensions", value=True)
    show_facts = st.sidebar.checkbox("Show Facts", value=True)
    show_kpis = st.sidebar.checkbox("Show KPIs", value=False)
    
    # Main content
    if not data['facts'] and not data['dimensions']:
        st.warning("No data available. Please check your data connection.")
        return
    
    # Key Metrics Row
    st.header("🎯 Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if 'fact_orders' in data['facts']:
            orders_df = data['facts']['fact_orders']
            total_orders = len(orders_df)
            create_metric_card("Total Orders", f"{total_orders:,}")
        else:
            create_metric_card("Total Orders", "N/A")
    
    with col2:
        if 'fact_orders' in data['facts'] and 'total_amount' in data['facts']['fact_orders'].columns:
            total_revenue = data['facts']['fact_orders']['total_amount'].sum()
            create_metric_card("Total Revenue", DataProcessor.format_currency(total_revenue))
        else:
            create_metric_card("Total Revenue", "N/A")
    
    with col3:
        if 'fact_performance' in data['facts']:
            performance_df = data['facts']['fact_performance']
            if 'vehicle_id' in performance_df.columns:
                unique_vehicles = performance_df['vehicle_id'].nunique()
                create_metric_card("Active Vehicles", f"{unique_vehicles:,}")
            else:
                create_metric_card("Active Vehicles", "N/A")
        else:
            create_metric_card("Active Vehicles", "N/A")
    
    with col4:
        if 'fact_inventory' in data['facts']:
            inventory_df = data['facts']['fact_inventory']
            if 'warehouse_id' in inventory_df.columns:
                unique_warehouses = inventory_df['warehouse_id'].nunique()
                create_metric_card("Warehouses", f"{unique_warehouses:,}")
            else:
                create_metric_card("Warehouses", "N/A")
        else:
            create_metric_card("Warehouses", "N/A")
    
    # Charts Section
    st.header("📈 Analytics Overview")
    
    # Revenue Analysis
    if 'fact_orders' in data['facts'] and 'total_amount' in data['facts']['fact_orders'].columns:
        st.subheader("💰 Revenue Analysis")
        
        orders_df = data['facts']['fact_orders'].copy()
        
        # Convert date column if it exists
        if 'order_date' in orders_df.columns:
            orders_df['order_date'] = pd.to_datetime(orders_df['order_date'])
            orders_df['month'] = orders_df['order_date'].dt.to_period('M')
            
            # Monthly revenue trend
            monthly_revenue = orders_df.groupby('month')['total_amount'].sum().reset_index()
            monthly_revenue['month_str'] = monthly_revenue['month'].astype(str)
            
            fig_revenue = ChartGenerator.create_line_chart(
                monthly_revenue, 'month_str', 'total_amount', 
                'Monthly Revenue Trend'
            )
            st.plotly_chart(fig_revenue, use_container_width=True)
        
        # Revenue by status
        if 'order_status' in orders_df.columns:
            revenue_by_status = orders_df.groupby('order_status')['total_amount'].sum().reset_index()
            fig_status = ChartGenerator.create_pie_chart(
                revenue_by_status, 'order_status', 'total_amount',
                'Revenue Distribution by Order Status'
            )
            st.plotly_chart(fig_status, use_container_width=True)
    
    # Performance Analysis
    if 'fact_performance' in data['facts']:
        st.subheader("🚛 Performance Analysis")
        
        performance_df = data['facts']['fact_performance'].copy()
        
        col1, col2 = st.columns(2)
        
        with col1:
            if 'fuel_consumed' in performance_df.columns and 'distance_traveled' in performance_df.columns:
                performance_df['fuel_efficiency'] = performance_df['distance_traveled'] / performance_df['fuel_consumed']
                
                # Top performing vehicles
                top_vehicles = performance_df.groupby('vehicle_id')['fuel_efficiency'].mean().reset_index()
                top_vehicles = top_vehicles.nlargest(10, 'fuel_efficiency')
                
                fig_vehicles = ChartGenerator.create_bar_chart(
                    top_vehicles, 'vehicle_id', 'fuel_efficiency',
                    'Top 10 Vehicles by Fuel Efficiency'
                )
                st.plotly_chart(fig_vehicles, use_container_width=True)
        
        with col2:
            if 'delivery_time' in performance_df.columns:
                # Delivery time distribution
                fig_delivery = px.histogram(
                    performance_df, x='delivery_time', 
                    title='Delivery Time Distribution',
                    nbins=20
                )
                st.plotly_chart(fig_delivery, use_container_width=True)
    
    # Inventory Analysis
    if 'fact_inventory' in data['facts']:
        st.subheader("📦 Inventory Analysis")
        
        inventory_df = data['facts']['fact_inventory'].copy()
        
        if 'quantity' in inventory_df.columns and 'warehouse_id' in inventory_df.columns:
            # Inventory by warehouse
            warehouse_inventory = inventory_df.groupby('warehouse_id')['quantity'].sum().reset_index()
            warehouse_inventory = warehouse_inventory.nlargest(10, 'quantity')
            
            fig_inventory = ChartGenerator.create_bar_chart(
                warehouse_inventory, 'warehouse_id', 'quantity',
                'Inventory Levels by Warehouse'
            )
            st.plotly_chart(fig_inventory, use_container_width=True)
    
    # Data Summary
    st.header("📋 Data Summary")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Dimensions")
        if data['dimensions']:
            for name, info in summary['dimensions'].items():
                st.write(f"**{name}**: {info['rows']:,} rows, {info['columns']} columns")
        else:
            st.write("No dimension data available")
    
    with col2:
        st.subheader("Facts")
        if data['facts']:
            for name, info in summary['facts'].items():
                st.write(f"**{name}**: {info['rows']:,} rows, {info['columns']} columns")
        else:
            st.write("No fact data available")
    
    # Footer
    st.markdown("---")
    st.markdown("**Data Source**: Azure Data Lake Storage Gen2 - Gold Layer")
    st.markdown(f"**Last Updated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
