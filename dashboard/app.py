"""
Main Streamlit Application
Supply Chain Analytics Dashboard
"""

import streamlit as st
import sys
import os
from streamlit_option_menu import option_menu

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import page modules
from pages import overview, revenue_analytics, performance_analytics, inventory_analytics, data_explorer

def main():
    """Main application function"""
    
    # Page configuration
    st.set_page_config(
        page_title="Supply Chain Analytics Dashboard",
        page_icon="🚛",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS
    st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    
    .sidebar .sidebar-content {
        background-color: #f8f9fa;
    }
    
    .stSelectbox > div > div {
        background-color: white;
    }
    
    .stButton > button {
        background-color: #1f77b4;
        color: white;
        border-radius: 0.5rem;
        border: none;
        padding: 0.5rem 1rem;
        font-weight: bold;
    }
    
    .stButton > button:hover {
        background-color: #0d5aa7;
    }
    
    .info-box {
        background-color: #e3f2fd;
        border: 1px solid #2196f3;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    
    .warning-box {
        background-color: #fff3e0;
        border: 1px solid #ff9800;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    
    .success-box {
        background-color: #e8f5e8;
        border: 1px solid #4caf50;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Main header
    st.markdown('<h1 class="main-header">🚛 Supply Chain Analytics Dashboard</h1>', unsafe_allow_html=True)
    
    # Sidebar navigation
    with st.sidebar:
        st.image("https://via.placeholder.com/200x100/1f77b4/ffffff?text=Supply+Chain", width=200)
        
        st.markdown("### Navigation")
        
        # Navigation menu
        selected_page = option_menu(
            menu_title=None,
            options=[
                "📊 Overview",
                "💰 Revenue Analytics", 
                "🚛 Performance Analytics",
                "📦 Inventory Analytics",
                "🔍 Data Explorer"
            ],
            icons=[
                "bar-chart",
                "currency-dollar",
                "truck",
                "box",
                "search"
            ],
            menu_icon="cast",
            default_index=0,
            styles={
                "container": {"padding": "0!important", "background-color": "#fafafa"},
                "icon": {"color": "#1f77b4", "font-size": "20px"},
                "nav-link": {
                    "font-size": "16px",
                    "text-align": "left",
                    "margin": "0px",
                    "--hover-color": "#eee"
                },
                "nav-link-selected": {"background-color": "#1f77b4"},
            }
        )
        
        st.markdown("---")
        
        # Data source info
        st.markdown("### 📊 Data Source")
        st.info("Azure Data Lake Storage Gen2 - Gold Layer")
        
        # Quick stats
        st.markdown("### 📈 Quick Stats")
        try:
            from data_connector import get_data_connector
            connector = get_data_connector()
            summary = connector.get_data_summary()
            
            total_dimensions = len(summary.get('dimensions', {}))
            total_facts = len(summary.get('facts', {}))
            total_kpis = len(summary.get('kpis', {}))
            
            st.metric("Dimensions", total_dimensions)
            st.metric("Facts", total_facts)
            st.metric("KPIs", total_kpis)
            
        except Exception as e:
            st.error("Unable to load data stats")
        
        st.markdown("---")
        
        # Help section
        st.markdown("### ❓ Help")
        st.markdown("""
        **Getting Started:**
        1. Select a page from the navigation menu
        2. Use filters in the sidebar to customize your view
        3. Interact with charts and tables
        4. Export data as needed
        """)
        
        # Contact info
        st.markdown("### 📞 Support")
        st.markdown("""
        **Technical Support:**
        - Email: support@company.com
        - Phone: +966-XX-XXX-XXXX
        - Documentation: [Link to docs]
        """)
    
    # Main content area
    try:
        if selected_page == "📊 Overview":
            overview.main()
        elif selected_page == "💰 Revenue Analytics":
            revenue_analytics.main()
        elif selected_page == "🚛 Performance Analytics":
            performance_analytics.main()
        elif selected_page == "📦 Inventory Analytics":
            inventory_analytics.main()
        elif selected_page == "🔍 Data Explorer":
            data_explorer.main()
        else:
            st.error("Page not found")
    
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        st.markdown("### Troubleshooting")
        st.markdown("""
        1. Check your internet connection
        2. Verify Azure Storage credentials
        3. Ensure data is available in the gold layer
        4. Contact support if the issue persists
        """)
    
    # Footer
    st.markdown("---")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**© 2025 Supply Chain Analytics**")
    
    with col2:
        st.markdown("**Powered by Streamlit & Azure**")
    
    with col3:
        st.markdown(f"**Last Updated: {st.session_state.get('last_updated', 'N/A')}**")

if __name__ == "__main__":
    main()
