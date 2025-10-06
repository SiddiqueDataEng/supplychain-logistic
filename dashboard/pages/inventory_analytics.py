"""
Inventory Analytics Dashboard Page
Inventory management and stock analysis
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_connector import get_data_connector
from utils import DataProcessor, ChartGenerator, KPICalculator, DataFilter, create_metric_card

def main():
    """Main inventory analytics dashboard function"""
    st.set_page_config(
        page_title="Supply Chain Analytics - Inventory",
        page_icon="📦",
        layout="wide"
    )
    
    st.title("📦 Inventory Analytics Dashboard")
    st.markdown("### Inventory Management & Stock Analysis")
    
    # Initialize data connector
    try:
        connector = get_data_connector()
        data = connector.get_analytics_data()
    except Exception as e:
        st.error(f"Failed to connect to data source: {str(e)}")
        return
    
    if 'fact_inventory' not in data['facts']:
        st.warning("No inventory data available for analysis.")
        return
    
    inventory_df = data['facts']['fact_inventory'].copy()
    
    # Sidebar filters
    st.sidebar.header("📅 Filters")
    
    # Date range filter
    if 'date' in inventory_df.columns:
        inventory_df['date'] = pd.to_datetime(inventory_df['date'])
        min_date = inventory_df['date'].min().date()
        max_date = inventory_df['date'].max().date()
        
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(max_date - timedelta(days=30), max_date),
            min_date=min_date,
            max_value=max_date
        )
        
        # Apply date filter
        if len(date_range) == 2:
            start_date, end_date = date_range
            inventory_df = DataFilter.filter_by_date_range(
                inventory_df, 'date', 
                pd.to_datetime(start_date), 
                pd.to_datetime(end_date)
            )
    
    # Warehouse filter
    if 'warehouse_id' in inventory_df.columns:
        warehouse_options = st.sidebar.multiselect(
            "Warehouse",
            options=inventory_df['warehouse_id'].unique(),
            default=[]
        )
        if warehouse_options:
            inventory_df = DataFilter.filter_by_category(inventory_df, 'warehouse_id', warehouse_options)
    
    # Item filter
    if 'item_id' in inventory_df.columns:
        item_options = st.sidebar.multiselect(
            "Item",
            options=inventory_df['item_id'].unique(),
            default=[]
        )
        if item_options:
            inventory_df = DataFilter.filter_by_category(inventory_df, 'item_id', item_options)
    
    # Calculate KPIs
    inventory_kpis = KPICalculator.calculate_inventory_kpis(inventory_df)
    
    # Key Inventory Metrics
    st.header("🎯 Inventory Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_quantity = inventory_kpis.get('total_quantity', 0)
        create_metric_card("Total Quantity", f"{total_quantity:,}")
    
    with col2:
        total_value = inventory_kpis.get('total_inventory_value', 0)
        create_metric_card("Total Inventory Value", DataProcessor.format_currency(total_value))
    
    with col3:
        avg_quantity = inventory_kpis.get('avg_quantity', 0)
        create_metric_card("Average Quantity", f"{avg_quantity:.0f}")
    
    with col4:
        if 'warehouse_id' in inventory_df.columns:
            unique_warehouses = inventory_df['warehouse_id'].nunique()
            create_metric_card("Active Warehouses", f"{unique_warehouses}")
        else:
            create_metric_card("Active Warehouses", "N/A")
    
    # Inventory Analysis
    st.header("📈 Inventory Analysis")
    
    # Inventory by Warehouse
    if 'warehouse_id' in inventory_df.columns and 'quantity' in inventory_df.columns:
        st.subheader("🏢 Inventory by Warehouse")
        
        warehouse_inventory = inventory_df.groupby('warehouse_id')['quantity'].sum().reset_index()
        warehouse_inventory = warehouse_inventory.sort_values('quantity', ascending=False)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_warehouse = ChartGenerator.create_bar_chart(
                warehouse_inventory, 'warehouse_id', 'quantity',
                'Inventory Levels by Warehouse'
            )
            st.plotly_chart(fig_warehouse, use_container_width=True)
        
        with col2:
            # Top 10 warehouses pie chart
            top_warehouses = warehouse_inventory.head(10)
            fig_warehouse_pie = ChartGenerator.create_pie_chart(
                top_warehouses, 'warehouse_id', 'quantity',
                'Top 10 Warehouses by Inventory'
            )
            st.plotly_chart(fig_warehouse_pie, use_container_width=True)
    
    # Inventory Value Analysis
    if 'quantity' in inventory_df.columns and 'unit_cost' in inventory_df.columns:
        st.subheader("💰 Inventory Value Analysis")
        
        # Calculate inventory value
        inventory_df['inventory_value'] = inventory_df['quantity'] * inventory_df['unit_cost']
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Value by warehouse
            if 'warehouse_id' in inventory_df.columns:
                warehouse_value = inventory_df.groupby('warehouse_id')['inventory_value'].sum().reset_index()
                warehouse_value = warehouse_value.sort_values('inventory_value', ascending=False)
                
                fig_value = ChartGenerator.create_bar_chart(
                    warehouse_value, 'warehouse_id', 'inventory_value',
                    'Inventory Value by Warehouse'
                )
                st.plotly_chart(fig_value, use_container_width=True)
        
        with col2:
            # Value distribution
            fig_value_dist = px.histogram(
                inventory_df, x='inventory_value',
                title='Inventory Value Distribution',
                nbins=30
            )
            fig_value_dist.update_layout(xaxis_title="Inventory Value (SAR)", yaxis_title="Count")
            st.plotly_chart(fig_value_dist, use_container_width=True)
    
    # Item Analysis
    if 'item_id' in inventory_df.columns:
        st.subheader("📦 Item Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Top items by quantity
            if 'quantity' in inventory_df.columns:
                item_quantity = inventory_df.groupby('item_id')['quantity'].sum().reset_index()
                top_items = item_quantity.nlargest(10, 'quantity')
                
                fig_items = ChartGenerator.create_bar_chart(
                    top_items, 'item_id', 'quantity',
                    'Top 10 Items by Quantity'
                )
                st.plotly_chart(fig_items, use_container_width=True)
        
        with col2:
            # Top items by value
            if 'inventory_value' in inventory_df.columns:
                item_value = inventory_df.groupby('item_id')['inventory_value'].sum().reset_index()
                top_value_items = item_value.nlargest(10, 'inventory_value')
                
                fig_value_items = ChartGenerator.create_bar_chart(
                    top_value_items, 'item_id', 'inventory_value',
                    'Top 10 Items by Value'
                )
                st.plotly_chart(fig_value_items, use_container_width=True)
    
    # Inventory Trends Over Time
    if 'date' in inventory_df.columns:
        st.subheader("📈 Inventory Trends Over Time")
        
        # Daily inventory metrics
        daily_inventory = inventory_df.groupby('date').agg({
            'quantity': 'sum',
            'inventory_value': 'sum' if 'inventory_value' in inventory_df.columns else 'count'
        }).reset_index()
        
        # Create subplots
        fig_trends = make_subplots(
            rows=1, cols=2,
            subplot_titles=['Total Quantity Over Time', 'Total Value Over Time'],
            vertical_spacing=0.1
        )
        
        fig_trends.add_trace(
            go.Scatter(x=daily_inventory['date'], y=daily_inventory['quantity'], 
                      name='Total Quantity', line=dict(color='blue')),
            row=1, col=1
        )
        
        if 'inventory_value' in daily_inventory.columns:
            fig_trends.add_trace(
                go.Scatter(x=daily_inventory['date'], y=daily_inventory['inventory_value'], 
                          name='Total Value', line=dict(color='green')),
                row=1, col=2
            )
        
        fig_trends.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_trends, use_container_width=True)
    
    # Stock Level Analysis
    if 'quantity' in inventory_df.columns:
        st.subheader("📊 Stock Level Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Quantity distribution
            fig_quantity_dist = px.histogram(
                inventory_df, x='quantity',
                title='Quantity Distribution',
                nbins=30
            )
            fig_quantity_dist.update_layout(xaxis_title="Quantity", yaxis_title="Count")
            st.plotly_chart(fig_quantity_dist, use_container_width=True)
        
        with col2:
            # Quantity statistics
            st.subheader("Quantity Statistics")
            
            stats_data = {
                'Metric': ['Mean', 'Median', 'Min', 'Max', 'Std Dev'],
                'Value': [
                    f"{inventory_df['quantity'].mean():.0f}",
                    f"{inventory_df['quantity'].median():.0f}",
                    f"{inventory_df['quantity'].min():.0f}",
                    f"{inventory_df['quantity'].max():.0f}",
                    f"{inventory_df['quantity'].std():.0f}"
                ]
            }
            
            stats_df = pd.DataFrame(stats_data)
            st.dataframe(stats_df, use_container_width=True)
    
    # Low Stock Analysis
    if 'quantity' in inventory_df.columns:
        st.subheader("⚠️ Low Stock Analysis")
        
        # Define low stock threshold (e.g., below 10th percentile)
        low_stock_threshold = inventory_df['quantity'].quantile(0.1)
        
        low_stock_items = inventory_df[inventory_df['quantity'] <= low_stock_threshold]
        
        if not low_stock_items.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**Low Stock Threshold**: {low_stock_threshold:.0f} units")
                st.write(f"**Items Below Threshold**: {len(low_stock_items)}")
            
            with col2:
                # Low stock by warehouse
                if 'warehouse_id' in low_stock_items.columns:
                    low_stock_by_warehouse = low_stock_items.groupby('warehouse_id').size().reset_index(name='count')
                    
                    fig_low_stock = ChartGenerator.create_bar_chart(
                        low_stock_by_warehouse, 'warehouse_id', 'count',
                        'Low Stock Items by Warehouse'
                    )
                    st.plotly_chart(fig_low_stock, use_container_width=True)
            
            # Low stock items table
            st.subheader("Low Stock Items Details")
            display_columns = ['item_id', 'warehouse_id', 'quantity', 'unit_cost']
            if 'inventory_value' in low_stock_items.columns:
                display_columns.append('inventory_value')
            
            available_columns = [col for col in display_columns if col in low_stock_items.columns]
            st.dataframe(low_stock_items[available_columns], use_container_width=True)
        else:
            st.success("No low stock items found!")
    
    # Inventory Turnover Analysis
    if 'quantity_sold' in inventory_df.columns and 'quantity' in inventory_df.columns:
        st.subheader("🔄 Inventory Turnover Analysis")
        
        # Calculate turnover ratio
        inventory_df['turnover_ratio'] = inventory_df['quantity_sold'] / inventory_df['quantity']
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Turnover by item
            item_turnover = inventory_df.groupby('item_id')['turnover_ratio'].mean().reset_index()
            top_turnover = item_turnover.nlargest(10, 'turnover_ratio')
            
            fig_turnover = ChartGenerator.create_bar_chart(
                top_turnover, 'item_id', 'turnover_ratio',
                'Top 10 Items by Turnover Ratio'
            )
            st.plotly_chart(fig_turnover, use_container_width=True)
        
        with col2:
            # Turnover distribution
            fig_turnover_dist = px.histogram(
                inventory_df, x='turnover_ratio',
                title='Turnover Ratio Distribution',
                nbins=20
            )
            fig_turnover_dist.update_layout(xaxis_title="Turnover Ratio", yaxis_title="Count")
            st.plotly_chart(fig_turnover_dist, use_container_width=True)
    
    # Inventory Summary Table
    st.header("📋 Inventory Summary")
    
    # Warehouse summary
    if 'warehouse_id' in inventory_df.columns:
        warehouse_summary = inventory_df.groupby('warehouse_id').agg({
            'quantity': 'sum',
            'inventory_value': 'sum' if 'inventory_value' in inventory_df.columns else 'count',
            'item_id': 'nunique' if 'item_id' in inventory_df.columns else 'count'
        }).reset_index()
        
        warehouse_summary.columns = ['Warehouse ID', 'Total Quantity', 'Total Value', 'Unique Items']
        
        # Round numeric columns
        numeric_columns = warehouse_summary.select_dtypes(include=['float64']).columns
        warehouse_summary[numeric_columns] = warehouse_summary[numeric_columns].round(2)
        
        st.subheader("Warehouse Summary")
        st.dataframe(warehouse_summary, use_container_width=True)
    
    # Item summary
    if 'item_id' in inventory_df.columns:
        item_summary = inventory_df.groupby('item_id').agg({
            'quantity': 'sum',
            'inventory_value': 'sum' if 'inventory_value' in inventory_df.columns else 'count',
            'warehouse_id': 'nunique' if 'warehouse_id' in inventory_df.columns else 'count'
        }).reset_index()
        
        item_summary.columns = ['Item ID', 'Total Quantity', 'Total Value', 'Warehouses']
        
        # Round numeric columns
        numeric_columns = item_summary.select_dtypes(include=['float64']).columns
        item_summary[numeric_columns] = item_summary[numeric_columns].round(2)
        
        st.subheader("Item Summary")
        st.dataframe(item_summary, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown("**Data Source**: Azure Data Lake Storage Gen2 - Gold Layer")
    if 'date' in inventory_df.columns:
        st.markdown(f"**Analysis Period**: {inventory_df['date'].min().strftime('%Y-%m-%d')} to {inventory_df['date'].max().strftime('%Y-%m-%d')}")
    st.markdown(f"**Last Updated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
