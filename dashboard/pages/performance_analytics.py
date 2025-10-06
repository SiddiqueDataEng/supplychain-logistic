"""
Performance Analytics Dashboard Page
Vehicle and operational performance analysis
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
    """Main performance analytics dashboard function"""
    st.set_page_config(
        page_title="Supply Chain Analytics - Performance",
        page_icon="🚛",
        layout="wide"
    )
    
    st.title("🚛 Performance Analytics Dashboard")
    st.markdown("### Vehicle & Operational Performance Analysis")
    
    # Initialize data connector
    try:
        connector = get_data_connector()
        data = connector.get_analytics_data()
    except Exception as e:
        st.error(f"Failed to connect to data source: {str(e)}")
        return
    
    # Get performance data
    performance_df = None
    fuel_df = None
    
    if 'fact_performance' in data['facts']:
        performance_df = data['facts']['fact_performance'].copy()
    
    if 'fact_fuel_consumption' in data['facts']:
        fuel_df = data['facts']['fact_fuel_consumption'].copy()
    
    if performance_df is None and fuel_df is None:
        st.warning("No performance data available for analysis.")
        return
    
    # Sidebar filters
    st.sidebar.header("📅 Filters")
    
    # Date range filter
    date_col = None
    if performance_df is not None and 'date' in performance_df.columns:
        date_col = 'date'
        performance_df['date'] = pd.to_datetime(performance_df['date'])
        min_date = performance_df['date'].min().date()
        max_date = performance_df['date'].max().date()
    elif fuel_df is not None and 'date' in fuel_df.columns:
        date_col = 'date'
        fuel_df['date'] = pd.to_datetime(fuel_df['date'])
        min_date = fuel_df['date'].min().date()
        max_date = fuel_df['date'].max().date()
    
    if date_col:
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(max_date - timedelta(days=30), max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        # Apply date filter
        if len(date_range) == 2:
            start_date, end_date = date_range
            if performance_df is not None:
                performance_df = DataFilter.filter_by_date_range(
                    performance_df, date_col, 
                    pd.to_datetime(start_date), 
                    pd.to_datetime(end_date)
                )
            if fuel_df is not None:
                fuel_df = DataFilter.filter_by_date_range(
                    fuel_df, date_col, 
                    pd.to_datetime(start_date), 
                    pd.to_datetime(end_date)
                )
    
    # Vehicle filter
    if performance_df is not None and 'vehicle_id' in performance_df.columns:
        vehicle_options = st.sidebar.multiselect(
            "Vehicle",
            options=performance_df['vehicle_id'].unique(),
            default=[]
        )
        if vehicle_options:
            performance_df = DataFilter.filter_by_category(performance_df, 'vehicle_id', vehicle_options)
    
    # Calculate KPIs
    performance_kpis = {}
    if performance_df is not None:
        performance_kpis = KPICalculator.calculate_performance_kpis(performance_df)
    
    # Key Performance Metrics
    st.header("🎯 Performance Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if 'avg_fuel_efficiency' in performance_kpis:
            create_metric_card(
                "Avg Fuel Efficiency", 
                f"{performance_kpis['avg_fuel_efficiency']:.2f} km/L"
            )
        else:
            create_metric_card("Avg Fuel Efficiency", "N/A")
    
    with col2:
        if 'avg_delivery_time' in performance_kpis:
            create_metric_card(
                "Avg Delivery Time", 
                f"{performance_kpis['avg_delivery_time']:.1f} hours"
            )
        else:
            create_metric_card("Avg Delivery Time", "N/A")
    
    with col3:
        if 'total_distance' in performance_kpis:
            create_metric_card(
                "Total Distance", 
                f"{performance_kpis['total_distance']:,.0f} km"
            )
        else:
            create_metric_card("Total Distance", "N/A")
    
    with col4:
        if 'avg_efficiency_score' in performance_kpis:
            create_metric_card(
                "Avg Efficiency Score", 
                f"{performance_kpis['avg_efficiency_score']:.1f}/10"
            )
        else:
            create_metric_card("Avg Efficiency Score", "N/A")
    
    # Performance Analysis
    if performance_df is not None:
        st.header("📈 Performance Analysis")
        
        # Fuel Efficiency Analysis
        if 'fuel_consumed' in performance_df.columns and 'distance_traveled' in performance_df.columns:
            st.subheader("⛽ Fuel Efficiency Analysis")
            
            # Calculate fuel efficiency
            performance_df['fuel_efficiency'] = performance_df['distance_traveled'] / performance_df['fuel_consumed']
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Top performing vehicles
                vehicle_efficiency = performance_df.groupby('vehicle_id')['fuel_efficiency'].mean().reset_index()
                top_vehicles = vehicle_efficiency.nlargest(10, 'fuel_efficiency')
                
                fig_vehicles = ChartGenerator.create_bar_chart(
                    top_vehicles, 'vehicle_id', 'fuel_efficiency',
                    'Top 10 Vehicles by Fuel Efficiency (km/L)'
                )
                st.plotly_chart(fig_vehicles, use_container_width=True)
            
            with col2:
                # Fuel efficiency distribution
                fig_dist = px.histogram(
                    performance_df, x='fuel_efficiency',
                    title='Fuel Efficiency Distribution',
                    nbins=20
                )
                fig_dist.update_layout(xaxis_title="Fuel Efficiency (km/L)", yaxis_title="Count")
                st.plotly_chart(fig_dist, use_container_width=True)
        
        # Delivery Performance
        if 'delivery_time' in performance_df.columns:
            st.subheader("🚚 Delivery Performance")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Delivery time by vehicle
                if 'vehicle_id' in performance_df.columns:
                    vehicle_delivery = performance_df.groupby('vehicle_id')['delivery_time'].mean().reset_index()
                    top_delivery = vehicle_delivery.nsmallest(10, 'delivery_time')
                    
                    fig_delivery = ChartGenerator.create_bar_chart(
                        top_delivery, 'vehicle_id', 'delivery_time',
                        'Top 10 Vehicles by Delivery Speed (hours)'
                    )
                    st.plotly_chart(fig_delivery, use_container_width=True)
            
            with col2:
                # Delivery time distribution
                fig_delivery_dist = px.histogram(
                    performance_df, x='delivery_time',
                    title='Delivery Time Distribution',
                    nbins=20
                )
                fig_delivery_dist.update_layout(xaxis_title="Delivery Time (hours)", yaxis_title="Count")
                st.plotly_chart(fig_delivery_dist, use_container_width=True)
        
        # Distance Analysis
        if 'distance_traveled' in performance_df.columns:
            st.subheader("🛣️ Distance Analysis")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Distance by vehicle
                if 'vehicle_id' in performance_df.columns:
                    vehicle_distance = performance_df.groupby('vehicle_id')['distance_traveled'].sum().reset_index()
                    top_distance = vehicle_distance.nlargest(10, 'distance_traveled')
                    
                    fig_distance = ChartGenerator.create_bar_chart(
                        top_distance, 'vehicle_id', 'distance_traveled',
                        'Top 10 Vehicles by Total Distance (km)'
                    )
                    st.plotly_chart(fig_distance, use_container_width=True)
            
            with col2:
                # Distance vs Fuel consumption scatter
                if 'fuel_consumed' in performance_df.columns:
                    fig_scatter = ChartGenerator.create_scatter_plot(
                        performance_df, 'distance_traveled', 'fuel_consumed',
                        'Distance vs Fuel Consumption',
                        size_col='fuel_efficiency' if 'fuel_efficiency' in performance_df.columns else None
                    )
                    st.plotly_chart(fig_scatter, use_container_width=True)
    
    # Fuel Consumption Analysis
    if fuel_df is not None:
        st.header("⛽ Fuel Consumption Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Fuel consumption by type
            if 'fuel_type' in fuel_df.columns and 'fuel_consumed' in fuel_df.columns:
                fuel_by_type = fuel_df.groupby('fuel_type')['fuel_consumed'].sum().reset_index()
                
                fig_fuel_type = ChartGenerator.create_pie_chart(
                    fuel_by_type, 'fuel_type', 'fuel_consumed',
                    'Fuel Consumption by Type'
                )
                st.plotly_chart(fig_fuel_type, use_container_width=True)
        
        with col2:
            # Daily fuel consumption trend
            if 'date' in fuel_df.columns and 'fuel_consumed' in fuel_df.columns:
                daily_fuel = fuel_df.groupby('date')['fuel_consumed'].sum().reset_index()
                
                fig_daily_fuel = ChartGenerator.create_line_chart(
                    daily_fuel, 'date', 'fuel_consumed',
                    'Daily Fuel Consumption Trend'
                )
                st.plotly_chart(fig_daily_fuel, use_container_width=True)
    
    # Efficiency Score Analysis
    if performance_df is not None and 'efficiency_score' in performance_df.columns:
        st.subheader("📊 Efficiency Score Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Efficiency score by vehicle
            if 'vehicle_id' in performance_df.columns:
                vehicle_efficiency = performance_df.groupby('vehicle_id')['efficiency_score'].mean().reset_index()
                top_efficiency = vehicle_efficiency.nlargest(10, 'efficiency_score')
                
                fig_efficiency = ChartGenerator.create_bar_chart(
                    top_efficiency, 'vehicle_id', 'efficiency_score',
                    'Top 10 Vehicles by Efficiency Score'
                )
                st.plotly_chart(fig_efficiency, use_container_width=True)
        
        with col2:
            # Efficiency score distribution
            fig_efficiency_dist = px.histogram(
                performance_df, x='efficiency_score',
                title='Efficiency Score Distribution',
                nbins=20
            )
            fig_efficiency_dist.update_layout(xaxis_title="Efficiency Score", yaxis_title="Count")
            st.plotly_chart(fig_efficiency_dist, use_container_width=True)
    
    # Performance Trends Over Time
    if performance_df is not None and 'date' in performance_df.columns:
        st.subheader("📈 Performance Trends Over Time")
        
        # Daily performance metrics
        daily_metrics = performance_df.groupby('date').agg({
            'fuel_efficiency': 'mean' if 'fuel_efficiency' in performance_df.columns else 'count',
            'delivery_time': 'mean' if 'delivery_time' in performance_df.columns else 'count',
            'distance_traveled': 'sum' if 'distance_traveled' in performance_df.columns else 'count',
            'efficiency_score': 'mean' if 'efficiency_score' in performance_df.columns else 'count'
        }).reset_index()
        
        # Create subplots
        fig_trends = make_subplots(
            rows=2, cols=2,
            subplot_titles=['Fuel Efficiency', 'Delivery Time', 'Distance Traveled', 'Efficiency Score'],
            vertical_spacing=0.1
        )
        
        if 'fuel_efficiency' in daily_metrics.columns:
            fig_trends.add_trace(
                go.Scatter(x=daily_metrics['date'], y=daily_metrics['fuel_efficiency'], 
                          name='Fuel Efficiency', line=dict(color='blue')),
                row=1, col=1
            )
        
        if 'delivery_time' in daily_metrics.columns:
            fig_trends.add_trace(
                go.Scatter(x=daily_metrics['date'], y=daily_metrics['delivery_time'], 
                          name='Delivery Time', line=dict(color='red')),
                row=1, col=2
            )
        
        if 'distance_traveled' in daily_metrics.columns:
            fig_trends.add_trace(
                go.Scatter(x=daily_metrics['date'], y=daily_metrics['distance_traveled'], 
                          name='Distance Traveled', line=dict(color='green')),
                row=2, col=1
            )
        
        if 'efficiency_score' in daily_metrics.columns:
            fig_trends.add_trace(
                go.Scatter(x=daily_metrics['date'], y=daily_metrics['efficiency_score'], 
                          name='Efficiency Score', line=dict(color='orange')),
                row=2, col=2
            )
        
        fig_trends.update_layout(height=600, showlegend=False)
        st.plotly_chart(fig_trends, use_container_width=True)
    
    # Performance Summary Table
    st.header("📋 Performance Summary")
    
    if performance_df is not None:
        # Vehicle performance summary
        if 'vehicle_id' in performance_df.columns:
            vehicle_summary = performance_df.groupby('vehicle_id').agg({
                'fuel_efficiency': 'mean' if 'fuel_efficiency' in performance_df.columns else 'count',
                'delivery_time': 'mean' if 'delivery_time' in performance_df.columns else 'count',
                'distance_traveled': 'sum' if 'distance_traveled' in performance_df.columns else 'count',
                'efficiency_score': 'mean' if 'efficiency_score' in performance_df.columns else 'count'
            }).reset_index()
            
            # Round numeric columns
            numeric_columns = vehicle_summary.select_dtypes(include=['float64']).columns
            vehicle_summary[numeric_columns] = vehicle_summary[numeric_columns].round(2)
            
            st.dataframe(vehicle_summary, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown("**Data Source**: Azure Data Lake Storage Gen2 - Gold Layer")
    if performance_df is not None and 'date' in performance_df.columns:
        st.markdown(f"**Analysis Period**: {performance_df['date'].min().strftime('%Y-%m-%d')} to {performance_df['date'].max().strftime('%Y-%m-%d')}")
    st.markdown(f"**Last Updated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
