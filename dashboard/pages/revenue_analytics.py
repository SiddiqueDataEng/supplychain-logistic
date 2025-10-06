"""
Revenue Analytics Dashboard Page
Detailed revenue analysis and financial metrics
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
    """Main revenue analytics dashboard function"""
    st.set_page_config(
        page_title="Supply Chain Analytics - Revenue",
        page_icon="💰",
        layout="wide"
    )
    
    st.title("💰 Revenue Analytics Dashboard")
    st.markdown("### Financial Performance & Revenue Analysis")
    
    # Initialize data connector
    try:
        connector = get_data_connector()
        data = connector.get_analytics_data()
    except Exception as e:
        st.error(f"Failed to connect to data source: {str(e)}")
        return
    
    if 'fact_orders' not in data['facts']:
        st.warning("No order data available for revenue analysis.")
        return
    
    orders_df = data['facts']['fact_orders'].copy()
    
    # Sidebar filters
    st.sidebar.header("📅 Filters")
    
    # Date range filter
    if 'order_date' in orders_df.columns:
        orders_df['order_date'] = pd.to_datetime(orders_df['order_date'])
        min_date = orders_df['order_date'].min().date()
        max_date = orders_df['order_date'].max().date()
        
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(max_date - timedelta(days=30), max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        # Apply date filter
        if len(date_range) == 2:
            start_date, end_date = date_range
            orders_df = DataFilter.filter_by_date_range(
                orders_df, 'order_date', 
                pd.to_datetime(start_date), 
                pd.to_datetime(end_date)
            )
    
    # Status filter
    if 'order_status' in orders_df.columns:
        status_options = st.sidebar.multiselect(
            "Order Status",
            options=orders_df['order_status'].unique(),
            default=orders_df['order_status'].unique()
        )
        if status_options:
            orders_df = DataFilter.filter_by_category(orders_df, 'order_status', status_options)
    
    # Customer filter
    if 'customer_id' in orders_df.columns:
        customer_options = st.sidebar.multiselect(
            "Customer",
            options=orders_df['customer_id'].unique(),
            default=[]
        )
        if customer_options:
            orders_df = DataFilter.filter_by_category(orders_df, 'customer_id', customer_options)
    
    # Calculate KPIs
    revenue_kpis = KPICalculator.calculate_revenue_kpis(orders_df)
    
    # Key Revenue Metrics
    st.header("🎯 Revenue Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_revenue = revenue_kpis.get('total_revenue', 0)
        create_metric_card("Total Revenue", DataProcessor.format_currency(total_revenue))
    
    with col2:
        avg_order_value = revenue_kpis.get('avg_order_value', 0)
        create_metric_card("Average Order Value", DataProcessor.format_currency(avg_order_value))
    
    with col3:
        total_orders = revenue_kpis.get('total_orders', 0)
        create_metric_card("Total Orders", f"{total_orders:,}")
    
    with col4:
        if total_orders > 0:
            daily_avg_revenue = total_revenue / 30  # Assuming 30-day period
            create_metric_card("Daily Avg Revenue", DataProcessor.format_currency(daily_avg_revenue))
        else:
            create_metric_card("Daily Avg Revenue", "N/A")
    
    # Revenue Trends
    st.header("📈 Revenue Trends")
    
    if 'order_date' in orders_df.columns and 'total_amount' in orders_df.columns:
        # Daily revenue trend
        orders_df['date'] = orders_df['order_date'].dt.date
        daily_revenue = orders_df.groupby('date')['total_amount'].sum().reset_index()
        
        fig_daily = ChartGenerator.create_line_chart(
            daily_revenue, 'date', 'total_amount',
            'Daily Revenue Trend'
        )
        st.plotly_chart(fig_daily, use_container_width=True)
        
        # Monthly revenue comparison
        orders_df['year_month'] = orders_df['order_date'].dt.to_period('M')
        monthly_revenue = orders_df.groupby('year_month')['total_amount'].sum().reset_index()
        monthly_revenue['month_str'] = monthly_revenue['year_month'].astype(str)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_monthly = ChartGenerator.create_bar_chart(
                monthly_revenue, 'month_str', 'total_amount',
                'Monthly Revenue Comparison'
            )
            st.plotly_chart(fig_monthly, use_container_width=True)
        
        with col2:
            # Revenue by day of week
            orders_df['day_of_week'] = orders_df['order_date'].dt.day_name()
            daily_avg = orders_df.groupby('day_of_week')['total_amount'].mean().reset_index()
            
            # Reorder days
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            daily_avg['day_of_week'] = pd.Categorical(daily_avg['day_of_week'], categories=day_order, ordered=True)
            daily_avg = daily_avg.sort_values('day_of_week')
            
            fig_dow = ChartGenerator.create_bar_chart(
                daily_avg, 'day_of_week', 'total_amount',
                'Average Revenue by Day of Week'
            )
            st.plotly_chart(fig_dow, use_container_width=True)
    
    # Revenue Analysis by Dimensions
    st.header("🔍 Revenue Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Revenue by order status
        if 'order_status' in orders_df.columns and 'total_amount' in orders_df.columns:
            status_revenue = orders_df.groupby('order_status')['total_amount'].sum().reset_index()
            status_revenue = status_revenue.sort_values('total_amount', ascending=False)
            
            fig_status = ChartGenerator.create_pie_chart(
                status_revenue, 'order_status', 'total_amount',
                'Revenue Distribution by Order Status'
            )
            st.plotly_chart(fig_status, use_container_width=True)
    
    with col2:
        # Top customers by revenue
        if 'customer_id' in orders_df.columns and 'total_amount' in orders_df.columns:
            customer_revenue = orders_df.groupby('customer_id')['total_amount'].sum().reset_index()
            top_customers = customer_revenue.nlargest(10, 'total_amount')
            
            fig_customers = ChartGenerator.create_bar_chart(
                top_customers, 'customer_id', 'total_amount',
                'Top 10 Customers by Revenue'
            )
            st.plotly_chart(fig_customers, use_container_width=True)
    
    # Order Value Analysis
    if 'total_amount' in orders_df.columns:
        st.subheader("📊 Order Value Distribution")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Order value histogram
            fig_hist = px.histogram(
                orders_df, x='total_amount', 
                title='Order Value Distribution',
                nbins=30
            )
            fig_hist.update_layout(xaxis_title="Order Value (SAR)", yaxis_title="Count")
            st.plotly_chart(fig_hist, use_container_width=True)
        
        with col2:
            # Order value statistics
            st.subheader("Order Value Statistics")
            
            stats_data = {
                'Metric': ['Mean', 'Median', 'Min', 'Max', 'Std Dev'],
                'Value': [
                    DataProcessor.format_currency(orders_df['total_amount'].mean()),
                    DataProcessor.format_currency(orders_df['total_amount'].median()),
                    DataProcessor.format_currency(orders_df['total_amount'].min()),
                    DataProcessor.format_currency(orders_df['total_amount'].max()),
                    DataProcessor.format_currency(orders_df['total_amount'].std())
                ]
            }
            
            stats_df = pd.DataFrame(stats_data)
            st.dataframe(stats_df, use_container_width=True)
    
    # Revenue Forecasting (Simple)
    if 'order_date' in orders_df.columns and 'total_amount' in orders_df.columns:
        st.subheader("🔮 Revenue Forecasting")
        
        # Simple moving average forecast
        daily_revenue = orders_df.groupby('order_date')['total_amount'].sum().reset_index()
        daily_revenue = daily_revenue.sort_values('order_date')
        
        # Calculate 7-day moving average
        daily_revenue['ma_7'] = daily_revenue['total_amount'].rolling(window=7).mean()
        
        # Simple forecast (last 7-day average)
        last_ma = daily_revenue['ma_7'].iloc[-1]
        
        # Create forecast for next 7 days
        last_date = daily_revenue['order_date'].iloc[-1]
        forecast_dates = [last_date + timedelta(days=i) for i in range(1, 8)]
        forecast_values = [last_ma] * 7
        
        forecast_df = pd.DataFrame({
            'date': forecast_dates,
            'forecast': forecast_values
        })
        
        # Combine actual and forecast
        actual_df = daily_revenue[['order_date', 'total_amount']].rename(columns={'order_date': 'date', 'total_amount': 'actual'})
        forecast_df['actual'] = None
        
        combined_df = pd.concat([actual_df, forecast_df], ignore_index=True)
        
        fig_forecast = go.Figure()
        
        # Actual data
        fig_forecast.add_trace(go.Scatter(
            x=actual_df['date'],
            y=actual_df['actual'],
            mode='lines',
            name='Actual Revenue',
            line=dict(color='blue')
        ))
        
        # Forecast data
        fig_forecast.add_trace(go.Scatter(
            x=forecast_df['date'],
            y=forecast_df['forecast'],
            mode='lines',
            name='Forecast (7-day MA)',
            line=dict(color='red', dash='dash')
        ))
        
        fig_forecast.update_layout(
            title='Revenue Forecast (7-Day Moving Average)',
            xaxis_title='Date',
            yaxis_title='Revenue (SAR)',
            hovermode='x unified'
        )
        
        st.plotly_chart(fig_forecast, use_container_width=True)
    
    # Data Table
    st.header("📋 Detailed Order Data")
    
    # Show sample of filtered data
    display_columns = ['order_id', 'customer_id', 'order_date', 'order_status', 'total_amount']
    available_columns = [col for col in display_columns if col in orders_df.columns]
    
    if available_columns:
        st.dataframe(
            orders_df[available_columns].head(100),
            use_container_width=True
        )
    else:
        st.info("No relevant columns available for display")
    
    # Footer
    st.markdown("---")
    st.markdown("**Data Source**: Azure Data Lake Storage Gen2 - Gold Layer")
    st.markdown(f"**Analysis Period**: {orders_df['order_date'].min().strftime('%Y-%m-%d')} to {orders_df['order_date'].max().strftime('%Y-%m-%d')}")
    st.markdown(f"**Last Updated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
