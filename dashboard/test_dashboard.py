"""
Test script for the Supply Chain Analytics Dashboard
"""

import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """Test if all required modules can be imported"""
    try:
        print("Testing imports...")
        
        # Test basic imports
        import streamlit as st
        print("✅ Streamlit imported successfully")
        
        import pandas as pd
        print("✅ Pandas imported successfully")
        
        import plotly.express as px
        print("✅ Plotly imported successfully")
        
        from azure.storage.blob import BlobServiceClient
        print("✅ Azure Storage Blob imported successfully")
        
        from streamlit_option_menu import option_menu
        print("✅ Streamlit Option Menu imported successfully")
        
        # Test dashboard modules
        from data_connector import get_data_connector
        print("✅ Data Connector imported successfully")
        
        from utils import DataProcessor, ChartGenerator
        print("✅ Utils imported successfully")
        
        # Test page modules
        from pages import overview, revenue_analytics, performance_analytics, inventory_analytics, data_explorer
        print("✅ All page modules imported successfully")
        
        print("\n🎉 All imports successful! Dashboard is ready to run.")
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def test_data_connector():
    """Test data connector functionality"""
    try:
        print("\nTesting data connector...")
        
        from data_connector import get_data_connector
        
        # This will test if the config file exists and is readable
        connector = get_data_connector()
        print("✅ Data connector initialized successfully")
        
        # Test getting data summary
        summary = connector.get_data_summary()
        print(f"✅ Data summary retrieved: {len(summary)} sections")
        
        return True
        
    except FileNotFoundError as e:
        print(f"⚠️  Config file not found: {e}")
        print("   Make sure 3-gold/gold_config.json exists")
        return False
    except Exception as e:
        print(f"❌ Data connector error: {e}")
        return False

def main():
    """Main test function"""
    print("🚛 Supply Chain Analytics Dashboard - Test Suite")
    print("=" * 50)
    
    # Test imports
    imports_ok = test_imports()
    
    # Test data connector
    connector_ok = test_data_connector()
    
    print("\n" + "=" * 50)
    print("📊 Test Results Summary:")
    print(f"   Imports: {'✅ PASS' if imports_ok else '❌ FAIL'}")
    print(f"   Data Connector: {'✅ PASS' if connector_ok else '❌ FAIL'}")
    
    if imports_ok and connector_ok:
        print("\n🎉 All tests passed! Dashboard is ready to run.")
        print("\nTo start the dashboard, run:")
        print("   streamlit run app.py")
        print("   or")
        print("   run_dashboard.bat")
    else:
        print("\n⚠️  Some tests failed. Please check the errors above.")
    
    return imports_ok and connector_ok

if __name__ == "__main__":
    main()
