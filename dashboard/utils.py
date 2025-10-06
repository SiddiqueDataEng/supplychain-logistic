"""
Utility functions for the Streamlit dashboard
"""

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)

class DataProcessor:
    """Utility class for data processing and analysis"""
    
    @staticmethod
    def format_currency(value: float, currency: str = "SAR") -> str:
        """Format value as currency"""
        if pd.isna(value):
            return "N/A"
        return f"{value:,.2f} {currency}"
    
    @staticmethod
    def format_number(value: float, decimals: int = 2) -> str:
        """Format number with appropriate decimal places"""
        if pd.isna(value):
            return "N/A"
        return f"{value:,.{decimals}f}"
    
    @staticmethod
    def format_percentage(value: float, decimals: int = 1) -> str:
        """Format value as percentage"""
        if pd.isna(value):
            return "N/A"
        return f"{value:.{decimals}f}%"
    
    @staticmethod
    def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
        """Safely divide two numbers, returning default if denominator is zero"""
        if pd.isna(numerator) or pd.isna(denominator) or denominator == 0:
            return default
        return numerator / denominator

class ChartGenerator:
    """Utility class for generating charts and visualizations"""
    
    @staticmethod
    def create_line_chart(df: pd.DataFrame, x_col: str, y_col: str, title: str, 
                         color_col: Optional[str] = None) -> go.Figure:
        """Create a line chart"""
        fig = px.line(df, x=x_col, y=y_col, color=color_col, title=title)
        fig.update_layout(
            xaxis_title=x_col.replace('_', ' ').title(),
            yaxis_title=y_col.replace('_', ' ').title(),
            hovermode='x unified'
        )
        return fig
    
    @staticmethod
    def create_bar_chart(df: pd.DataFrame, x_col: str, y_col: str, title: str,
                        color_col: Optional[str] = None) -> go.Figure:
        """Create a bar chart"""
        fig = px.bar(df, x=x_col, y=y_col, color=color_col, title=title)
        fig.update_layout(
            xaxis_title=x_col.replace('_', ' ').title(),
            yaxis_title=y_col.replace('_', ' ').title(),
            hovermode='x unified'
        )
        return fig
    
    @staticmethod
    def create_pie_chart(df: pd.DataFrame, names_col: str, values_col: str, title: str) -> go.Figure:
        """Create a pie chart"""
        fig = px.pie(df, names=names_col, values=values_col, title=title)
        fig.update_traces(textposition='inside', textinfo='percent+label')
        return fig
    
    @staticmethod
    def create_scatter_plot(df: pd.DataFrame, x_col: str, y_col: str, title: str,
                           color_col: Optional[str] = None, size_col: Optional[str] = None) -> go.Figure:
        """Create a scatter plot"""
        fig = px.scatter(df, x=x_col, y=y_col, color=color_col, size=size_col, title=title)
        fig.update_layout(
            xaxis_title=x_col.replace('_', ' ').title(),
            yaxis_title=y_col.replace('_', ' ').title()
        )
        return fig
    
    @staticmethod
    def create_heatmap(df: pd.DataFrame, x_col: str, y_col: str, values_col: str, title: str) -> go.Figure:
        """Create a heatmap"""
        pivot_df = df.pivot_table(values=values_col, index=y_col, columns=x_col, aggfunc='mean')
        fig = px.imshow(pivot_df, title=title, aspect="auto")
        return fig
    
    @staticmethod
    def create_gauge_chart(value: float, max_value: float, title: str, 
                          color: str = "green") -> go.Figure:
        """Create a gauge chart"""
        fig = go.Figure(go.Indicator(
            mode = "gauge+number+delta",
            value = value,
            domain = {'x': [0, 1], 'y': [0, 1]},
            title = {'text': title},
            delta = {'reference': max_value * 0.8},
            gauge = {
                'axis': {'range': [None, max_value]},
                'bar': {'color': color},
                'steps': [
                    {'range': [0, max_value * 0.5], 'color': "lightgray"},
                    {'range': [max_value * 0.5, max_value * 0.8], 'color': "gray"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': max_value * 0.9
                }
            }
        ))
        return fig

class KPICalculator:
    """Utility class for calculating KPIs and metrics"""
    
    @staticmethod
    def calculate_revenue_kpis(orders_df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate revenue-related KPIs"""
        if orders_df.empty:
            return {}
        
        # Ensure date column is datetime
        if 'order_date' in orders_df.columns:
            orders_df['order_date'] = pd.to_datetime(orders_df['order_date'])
            orders_df['year_month'] = orders_df['order_date'].dt.to_period('M')
        
        kpis = {}
        
        # Total revenue
        if 'total_amount' in orders_df.columns:
            kpis['total_revenue'] = orders_df['total_amount'].sum()
            kpis['avg_order_value'] = orders_df['total_amount'].mean()
            kpis['max_order_value'] = orders_df['total_amount'].max()
            kpis['min_order_value'] = orders_df['total_amount'].min()
        
        # Order count
        kpis['total_orders'] = len(orders_df)
        
        # Monthly revenue trend
        if 'year_month' in orders_df.columns and 'total_amount' in orders_df.columns:
            monthly_revenue = orders_df.groupby('year_month')['total_amount'].sum().reset_index()
            kpis['monthly_revenue'] = monthly_revenue
        
        # Revenue by status
        if 'order_status' in orders_df.columns and 'total_amount' in orders_df.columns:
            revenue_by_status = orders_df.groupby('order_status')['total_amount'].sum().reset_index()
            kpis['revenue_by_status'] = revenue_by_status
        
        return kpis
    
    @staticmethod
    def calculate_performance_kpis(performance_df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate performance-related KPIs"""
        if performance_df.empty:
            return {}
        
        kpis = {}
        
        # Fuel efficiency
        if 'fuel_consumed' in performance_df.columns and 'distance_traveled' in performance_df.columns:
            performance_df['fuel_efficiency'] = performance_df['distance_traveled'] / performance_df['fuel_consumed']
            kpis['avg_fuel_efficiency'] = performance_df['fuel_efficiency'].mean()
            kpis['max_fuel_efficiency'] = performance_df['fuel_efficiency'].max()
            kpis['min_fuel_efficiency'] = performance_df['fuel_efficiency'].min()
        
        # Delivery performance
        if 'delivery_time' in performance_df.columns:
            kpis['avg_delivery_time'] = performance_df['delivery_time'].mean()
            kpis['max_delivery_time'] = performance_df['delivery_time'].max()
            kpis['min_delivery_time'] = performance_df['delivery_time'].min()
        
        # Distance metrics
        if 'distance_traveled' in performance_df.columns:
            kpis['total_distance'] = performance_df['distance_traveled'].sum()
            kpis['avg_distance'] = performance_df['distance_traveled'].mean()
        
        # Efficiency score
        if 'efficiency_score' in performance_df.columns:
            kpis['avg_efficiency_score'] = performance_df['efficiency_score'].mean()
            kpis['max_efficiency_score'] = performance_df['efficiency_score'].max()
        
        return kpis
    
    @staticmethod
    def calculate_inventory_kpis(inventory_df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate inventory-related KPIs"""
        if inventory_df.empty:
            return {}
        
        kpis = {}
        
        # Stock levels
        if 'quantity' in inventory_df.columns:
            kpis['total_quantity'] = inventory_df['quantity'].sum()
            kpis['avg_quantity'] = inventory_df['quantity'].mean()
            kpis['max_quantity'] = inventory_df['quantity'].max()
            kpis['min_quantity'] = inventory_df['quantity'].min()
        
        # Inventory value
        if 'quantity' in inventory_df.columns and 'unit_cost' in inventory_df.columns:
            inventory_df['inventory_value'] = inventory_df['quantity'] * inventory_df['unit_cost']
            kpis['total_inventory_value'] = inventory_df['inventory_value'].sum()
            kpis['avg_inventory_value'] = inventory_df['inventory_value'].mean()
        
        # Stock turnover (if we have sales data)
        if 'quantity_sold' in inventory_df.columns and 'quantity' in inventory_df.columns:
            kpis['avg_turnover_ratio'] = (inventory_df['quantity_sold'] / inventory_df['quantity']).mean()
        
        return kpis

class DataFilter:
    """Utility class for filtering and querying data"""
    
    @staticmethod
    def filter_by_date_range(df: pd.DataFrame, date_col: str, start_date: datetime, 
                           end_date: datetime) -> pd.DataFrame:
        """Filter DataFrame by date range"""
        if date_col not in df.columns:
            return df
        
        df[date_col] = pd.to_datetime(df[date_col])
        return df[(df[date_col] >= start_date) & (df[date_col] <= end_date)]
    
    @staticmethod
    def filter_by_category(df: pd.DataFrame, category_col: str, selected_categories: List[str]) -> pd.DataFrame:
        """Filter DataFrame by category values"""
        if category_col not in df.columns or not selected_categories:
            return df
        
        return df[df[category_col].isin(selected_categories)]
    
    @staticmethod
    def get_top_n(df: pd.DataFrame, group_col: str, value_col: str, n: int = 10) -> pd.DataFrame:
        """Get top N records by value"""
        if group_col not in df.columns or value_col not in df.columns:
            return df
        
        return df.nlargest(n, value_col)

def create_metric_card(title: str, value: str, delta: Optional[str] = None, 
                      delta_color: str = "normal") -> None:
    """Create a metric card in Streamlit"""
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        st.metric(
            label=title,
            value=value,
            delta=delta,
            delta_color=delta_color
        )

def create_info_box(title: str, content: str, icon: str = "ℹ️") -> None:
    """Create an information box"""
    st.info(f"{icon} **{title}**\n\n{content}")

def create_warning_box(title: str, content: str) -> None:
    """Create a warning box"""
    st.warning(f"⚠️ **{title}**\n\n{content}")

def create_success_box(title: str, content: str) -> None:
    """Create a success box"""
    st.success(f"✅ **{title}**\n\n{content}")
