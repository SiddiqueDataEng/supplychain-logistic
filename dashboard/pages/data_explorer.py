"""
Data Explorer Dashboard Page
Interactive data exploration and analysis tools
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
from utils import DataProcessor, ChartGenerator, DataFilter, create_info_box

def main():
    """Main data explorer dashboard function"""
    st.set_page_config(
        page_title="Supply Chain Analytics - Data Explorer",
        page_icon="🔍",
        layout="wide"
    )
    
    st.title("🔍 Data Explorer")
    st.markdown("### Interactive Data Exploration & Analysis Tools")
    
    # Initialize data connector
    try:
        connector = get_data_connector()
        data = connector.get_analytics_data()
    except Exception as e:
        st.error(f"Failed to connect to data source: {str(e)}")
        return
    
    # Data source selection
    st.sidebar.header("📊 Data Source")
    
    # Combine all data sources
    all_data = {}
    all_data.update(data['dimensions'])
    all_data.update(data['facts'])
    all_data.update(data['kpis'])
    
    if not all_data:
        st.warning("No data available for exploration.")
        return
    
    # Data source selector
    data_source = st.sidebar.selectbox(
        "Select Data Source",
        options=list(all_data.keys()),
        help="Choose a dataset to explore"
    )
    
    if data_source not in all_data:
        st.error(f"Data source '{data_source}' not found.")
        return
    
    df = all_data[data_source].copy()
    
    # Data info
    st.header(f"📋 Dataset: {data_source}")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Rows", f"{len(df):,}")
    
    with col2:
        st.metric("Columns", f"{len(df.columns)}")
    
    with col3:
        st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    
    with col4:
        missing_pct = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
        st.metric("Missing Data", f"{missing_pct:.1f}%")
    
    # Data preview
    st.subheader("📖 Data Preview")
    
    # Show first few rows
    st.dataframe(df.head(10), use_container_width=True)
    
    # Data types and info
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Data Types")
        dtype_df = pd.DataFrame({
            'Column': df.columns,
            'Type': df.dtypes.astype(str),
            'Non-Null Count': df.count(),
            'Null Count': df.isnull().sum()
        })
        st.dataframe(dtype_df, use_container_width=True)
    
    with col2:
        st.subheader("📈 Basic Statistics")
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            st.dataframe(df[numeric_cols].describe(), use_container_width=True)
        else:
            st.info("No numeric columns found for statistics")
    
    # Interactive filters
    st.header("🔧 Interactive Filters")
    
    # Date filter
    date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
    if date_columns:
        date_col = st.selectbox("Select Date Column", date_columns)
        
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            min_date = df[date_col].min().date()
            max_date = df[date_col].max().date()
            
            date_range = st.date_input(
                f"Filter by {date_col}",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date
            )
            
            if len(date_range) == 2:
                start_date, end_date = date_range
                df = DataFilter.filter_by_date_range(
                    df, date_col, 
                    pd.to_datetime(start_date), 
                    pd.to_datetime(end_date)
                )
    
    # Categorical filters
    categorical_columns = df.select_dtypes(include=['object', 'category']).columns
    for col in categorical_columns[:5]:  # Limit to first 5 categorical columns
        unique_values = df[col].unique()
        if len(unique_values) <= 20:  # Only show filters for columns with reasonable number of unique values
            selected_values = st.multiselect(
                f"Filter by {col}",
                options=unique_values,
                default=unique_values
            )
            if selected_values:
                df = DataFilter.filter_by_category(df, col, selected_values)
    
    # Visualization section
    st.header("📊 Data Visualization")
    
    # Chart type selection
    chart_type = st.selectbox(
        "Select Chart Type",
        ["Bar Chart", "Line Chart", "Scatter Plot", "Histogram", "Pie Chart", "Box Plot", "Heatmap"]
    )
    
    # Column selection for visualization
    col1, col2 = st.columns(2)
    
    with col1:
        x_column = st.selectbox("X-axis Column", df.columns)
    
    with col2:
        y_column = st.selectbox("Y-axis Column", df.columns)
    
    # Additional options based on chart type
    color_column = None
    size_column = None
    
    if chart_type in ["Scatter Plot", "Bar Chart", "Line Chart"]:
        color_column = st.selectbox("Color Column (Optional)", ["None"] + list(df.columns))
        if color_column == "None":
            color_column = None
    
    if chart_type == "Scatter Plot":
        size_column = st.selectbox("Size Column (Optional)", ["None"] + list(df.select_dtypes(include=['number']).columns))
        if size_column == "None":
            size_column = None
    
    # Generate chart
    if st.button("Generate Chart"):
        try:
            if chart_type == "Bar Chart":
                if df[x_column].dtype in ['object', 'category']:
                    # Group by categorical column
                    chart_data = df.groupby(x_column)[y_column].sum().reset_index()
                    fig = ChartGenerator.create_bar_chart(chart_data, x_column, y_column, f"{y_column} by {x_column}")
                else:
                    fig = ChartGenerator.create_bar_chart(df, x_column, y_column, f"{y_column} by {x_column}", color_column)
                st.plotly_chart(fig, use_container_width=True)
            
            elif chart_type == "Line Chart":
                fig = ChartGenerator.create_line_chart(df, x_column, y_column, f"{y_column} over {x_column}", color_column)
                st.plotly_chart(fig, use_container_width=True)
            
            elif chart_type == "Scatter Plot":
                fig = ChartGenerator.create_scatter_plot(df, x_column, y_column, f"{y_column} vs {x_column}", color_column, size_column)
                st.plotly_chart(fig, use_container_width=True)
            
            elif chart_type == "Histogram":
                fig = px.histogram(df, x=x_column, color=color_column, title=f"Distribution of {x_column}")
                st.plotly_chart(fig, use_container_width=True)
            
            elif chart_type == "Pie Chart":
                if df[x_column].dtype in ['object', 'category']:
                    pie_data = df.groupby(x_column)[y_column].sum().reset_index()
                    fig = ChartGenerator.create_pie_chart(pie_data, x_column, y_column, f"{y_column} Distribution")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Pie chart requires a categorical column for grouping")
            
            elif chart_type == "Box Plot":
                fig = px.box(df, x=x_column, y=y_column, color=color_column, title=f"Box Plot: {y_column} by {x_column}")
                st.plotly_chart(fig, use_container_width=True)
            
            elif chart_type == "Heatmap":
                if df[x_column].dtype in ['object', 'category'] and df[y_column].dtype in ['object', 'category']:
                    # Create pivot table for heatmap
                    pivot_data = df.groupby([x_column, y_column]).size().reset_index(name='count')
                    fig = ChartGenerator.create_heatmap(pivot_data, x_column, y_column, 'count', f"Heatmap: {x_column} vs {y_column}")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Heatmap requires two categorical columns")
        
        except Exception as e:
            st.error(f"Error generating chart: {str(e)}")
    
    # Correlation analysis
    st.header("🔗 Correlation Analysis")
    
    numeric_df = df.select_dtypes(include=['number'])
    if len(numeric_df.columns) > 1:
        correlation_matrix = numeric_df.corr()
        
        fig_corr = px.imshow(
            correlation_matrix,
            title="Correlation Matrix",
            aspect="auto",
            color_continuous_scale="RdBu_r"
        )
        st.plotly_chart(fig_corr, use_container_width=True)
        
        # Correlation table
        st.subheader("Correlation Values")
        st.dataframe(correlation_matrix.round(3), use_container_width=True)
    else:
        st.info("Not enough numeric columns for correlation analysis")
    
    # Data quality analysis
    st.header("🔍 Data Quality Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Missing data analysis
        missing_data = df.isnull().sum()
        missing_data = missing_data[missing_data > 0].sort_values(ascending=False)
        
        if len(missing_data) > 0:
            st.subheader("Missing Data by Column")
            fig_missing = px.bar(
                x=missing_data.index,
                y=missing_data.values,
                title="Missing Data Count by Column"
            )
            fig_missing.update_layout(xaxis_title="Column", yaxis_title="Missing Count")
            st.plotly_chart(fig_missing, use_container_width=True)
        else:
            st.success("No missing data found!")
    
    with col2:
        # Duplicate analysis
        duplicate_count = df.duplicated().sum()
        st.subheader("Duplicate Analysis")
        st.metric("Duplicate Rows", duplicate_count)
        
        if duplicate_count > 0:
            st.warning(f"Found {duplicate_count} duplicate rows")
        else:
            st.success("No duplicate rows found!")
    
    # Advanced analysis
    st.header("🧮 Advanced Analysis")
    
    # Statistical tests
    if len(numeric_df.columns) >= 2:
        st.subheader("Statistical Tests")
        
        test_col1 = st.selectbox("First Column for Test", numeric_df.columns)
        test_col2 = st.selectbox("Second Column for Test", numeric_df.columns)
        
        if st.button("Run Correlation Test"):
            try:
                from scipy.stats import pearsonr
                correlation, p_value = pearsonr(df[test_col1].dropna(), df[test_col2].dropna())
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Correlation", f"{correlation:.3f}")
                with col2:
                    st.metric("P-value", f"{p_value:.3f}")
                with col3:
                    significance = "Significant" if p_value < 0.05 else "Not Significant"
                    st.metric("Significance", significance)
            except Exception as e:
                st.error(f"Error running correlation test: {str(e)}")
    
    # Data export
    st.header("💾 Data Export")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Download as CSV"):
            csv = df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"{data_source}_export.csv",
                mime="text/csv"
            )
    
    with col2:
        if st.button("Download as Excel"):
            excel = df.to_excel(index=False)
            st.download_button(
                label="Download Excel",
                data=excel,
                file_name=f"{data_source}_export.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    
    # Footer
    st.markdown("---")
    st.markdown("**Data Source**: Azure Data Lake Storage Gen2 - Gold Layer")
    st.markdown(f"**Dataset**: {data_source}")
    st.markdown(f"**Last Updated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
