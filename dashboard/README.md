# Supply Chain Analytics Dashboard

A comprehensive Streamlit-based analytics dashboard for supply chain data analysis, built on Azure Data Lake Storage Gen2 gold layer data.

## 🚀 Features

### 📊 Multi-Page Dashboard
- **Overview**: Key metrics and high-level analytics
- **Revenue Analytics**: Financial performance and revenue analysis
- **Performance Analytics**: Vehicle and operational performance metrics
- **Inventory Analytics**: Stock management and inventory analysis
- **Data Explorer**: Interactive data exploration tools

### 🔧 Advanced Analytics
- Real-time data visualization with Plotly
- Interactive filtering and drill-down capabilities
- KPI calculations and trend analysis
- Data quality assessment
- Export functionality (CSV, Excel)

### 🏗️ Architecture
- **Data Source**: Azure Data Lake Storage Gen2 - Gold Layer
- **Frontend**: Streamlit with custom CSS styling
- **Visualization**: Plotly, Altair, Matplotlib
- **Data Processing**: Pandas, NumPy
- **Cloud Integration**: Azure Storage Blob SDK

## 📋 Prerequisites

- Python 3.8 or higher
- Azure Storage account with gold layer data
- Internet connection for data access

## 🛠️ Installation

1. **Clone or download the dashboard files**
   ```bash
   cd dashboard
   ```

2. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Azure connection**
   - Ensure `3-gold/gold_config.json` contains valid Azure Storage credentials
   - Verify gold layer data is available in Azure Storage

## 🚀 Running the Dashboard

### Option 1: Using Batch File (Windows)
```bash
run_dashboard.bat
```

### Option 2: Using Command Line
```bash
streamlit run app.py --server.port 8501
```

### Option 3: Custom Configuration
```bash
streamlit run app.py --server.port 8501 --server.address 0.0.0.0
```

## 📊 Dashboard Pages

### 1. Overview Dashboard
- **Key Metrics**: Total orders, revenue, vehicles, warehouses
- **Revenue Trends**: Monthly revenue analysis and forecasting
- **Performance Overview**: Vehicle efficiency and delivery metrics
- **Inventory Summary**: Stock levels and warehouse distribution

### 2. Revenue Analytics
- **Financial KPIs**: Revenue, AOV, order counts
- **Trend Analysis**: Daily, monthly, and seasonal patterns
- **Customer Analysis**: Top customers and revenue distribution
- **Forecasting**: Simple moving average predictions

### 3. Performance Analytics
- **Vehicle Metrics**: Fuel efficiency, delivery times, distance
- **Operational KPIs**: Performance scores and efficiency ratings
- **Trend Analysis**: Performance over time
- **Comparative Analysis**: Vehicle and route comparisons

### 4. Inventory Analytics
- **Stock Management**: Quantity levels and value analysis
- **Warehouse Analysis**: Distribution and capacity utilization
- **Item Performance**: Top items and turnover analysis
- **Low Stock Alerts**: Automated threshold monitoring

### 5. Data Explorer
- **Interactive Exploration**: Custom chart generation
- **Data Quality**: Missing data and duplicate analysis
- **Statistical Analysis**: Correlation and statistical tests
- **Export Tools**: CSV and Excel download options

## 🔧 Configuration

### Azure Storage Configuration
The dashboard reads configuration from `../3-gold/gold_config.json`:

```json
{
  "azure_storage": {
    "account_name": "your-storage-account",
    "connection_string": "your-connection-string",
    "gold_container": "gold"
  }
}
```

### Customization Options
- **Date Ranges**: Configurable analysis periods
- **Filters**: Multi-dimensional filtering capabilities
- **Charts**: Customizable visualization options
- **KPIs**: Configurable metric calculations

## 📈 Data Sources

### Dimensions
- `dim_customers`: Customer information
- `dim_vehicles`: Vehicle details
- `dim_warehouses`: Warehouse information
- `dim_suppliers`: Supplier data
- `dim_time`: Time dimension

### Facts
- `fact_orders`: Order transactions
- `fact_performance`: Vehicle performance metrics
- `fact_fuel_consumption`: Fuel usage data
- `fact_inventory`: Inventory levels

### KPIs
- Revenue metrics
- Performance indicators
- Inventory turnover
- Supply chain efficiency

## 🎨 Customization

### Adding New Pages
1. Create a new Python file in `pages/` directory
2. Follow the existing page structure
3. Add navigation item in `app.py`

### Custom Visualizations
1. Use `utils.py` ChartGenerator class
2. Extend with new chart types
3. Add to page modules as needed

### Data Processing
1. Extend `utils.py` DataProcessor class
2. Add new KPI calculations
3. Implement custom filters

## 🔍 Troubleshooting

### Common Issues

1. **Connection Error**
   - Verify Azure Storage credentials
   - Check network connectivity
   - Ensure gold layer data exists

2. **Data Loading Issues**
   - Check data format compatibility
   - Verify column names match expected schema
   - Review data quality

3. **Performance Issues**
   - Reduce data volume with filters
   - Use caching for large datasets
   - Optimize query patterns

### Debug Mode
Run with debug information:
```bash
streamlit run app.py --logger.level debug
```

## 📚 API Reference

### Data Connector
- `get_data_connector()`: Initialize Azure connection
- `get_dimensions()`: Load dimension tables
- `get_facts()`: Load fact tables
- `get_kpis()`: Load KPI data

### Utilities
- `DataProcessor`: Data formatting and processing
- `ChartGenerator`: Visualization creation
- `KPICalculator`: Metric calculations
- `DataFilter`: Data filtering operations

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📞 Support

For technical support or questions:
- Email: support@company.com
- Documentation: [Link to full docs]
- Issues: [GitHub Issues]

## 🔄 Version History

- **v1.0.0**: Initial release with basic analytics
- **v1.1.0**: Added performance analytics
- **v1.2.0**: Enhanced inventory management
- **v1.3.0**: Interactive data explorer

---

**Built with ❤️ using Streamlit and Azure Data Lake Storage Gen2**
