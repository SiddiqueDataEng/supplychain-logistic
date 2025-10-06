# Gold Layer Processor for Azure Data Lake Storage Gen2

## Overview

The Gold Layer Processor transforms Silver layer data into analytics-ready dimensional models with facts, dimensions, and KPIs. This is the final layer of the medallion architecture, providing business-ready data optimized for Power BI, Databricks, and other analytics tools.

## Features

- **Dimensional Modeling**: Creates star schema with facts and dimensions
- **KPI Calculations**: Comprehensive business metrics and KPIs
- **Analytics Views**: Pre-built views for Power BI and Databricks
- **Azure ADLS Gen2 Integration**: Direct integration with Azure Data Lake Storage Gen2
- **Data Quality**: Built-in data quality checks and validation
- **Metadata Tracking**: Complete processing metadata and lineage
- **Modular Processing**: Process dimensions, facts, or KPIs independently

## Architecture

```
Silver Layer (Azure ADLS Gen2)
    ↓
Gold Layer Processor
    ↓
Dimensional Modeling:
    - Dimension Tables
    - Fact Tables
    - KPI Calculations
    - Analytics Views
    ↓
Gold Layer (Azure ADLS Gen2)
    ↓
Power BI / Databricks / Analytics Tools
```

## Files Structure

- `gold_layer_processor.py` - Core processor with dimensional modeling logic
- `gold_layer_launcher.py` - Command-line interface
- `gold_config.json` - Configuration file
- `run_gold_layer.bat` - Windows batch script for full processing
- `check_gold_status.bat` - Windows batch script for status check
- `create_dimensions_only.bat` - Windows batch script for dimensions only
- `create_facts_only.bat` - Windows batch script for facts only
- `calculate_kpis_only.bat` - Windows batch script for KPIs only

## Dimensional Model

### Dimension Tables

#### 1. dim_customer
- **Purpose**: Customer master data
- **Key**: customer_key (surrogate key)
- **Attributes**: customer_id, customer_name, customer_email, customer_phone, customer_type

#### 2. dim_product
- **Purpose**: Product master data
- **Key**: product_key (surrogate key)
- **Attributes**: product_id, product_name, product_category, product_price, product_weight

#### 3. dim_vehicle
- **Purpose**: Vehicle master data
- **Key**: vehicle_key (surrogate key)
- **Attributes**: vehicle_id, vehicle_type, make, model, year, capacity, fuel_type

#### 4. dim_supplier
- **Purpose**: Supplier master data
- **Key**: supplier_key (surrogate key)
- **Attributes**: supplier_id, supplier_name, contact_person, email, phone, rating

#### 5. dim_warehouse
- **Purpose**: Warehouse master data
- **Key**: warehouse_key (surrogate key)
- **Attributes**: warehouse_id, warehouse_name, location, city, country, capacity

#### 6. dim_geography
- **Purpose**: Geographic master data
- **Key**: geography_key (surrogate key)
- **Attributes**: city, country, latitude, longitude, region

#### 7. dim_time
- **Purpose**: Time dimension for temporal analysis
- **Key**: date_key (YYYYMMDD format)
- **Attributes**: date, year, quarter, month, month_name, week, day_of_year, day_of_month, day_of_week, day_name, is_weekend, is_holiday, fiscal_year, fiscal_quarter

### Fact Tables

#### 1. fact_orders
- **Purpose**: Order transactions
- **Measures**: quantity, unit_price, total_amount
- **Foreign Keys**: customer_id, product_id, order_date

#### 2. fact_performance
- **Purpose**: Vehicle performance metrics
- **Measures**: distance_traveled, fuel_consumed, delivery_time, efficiency_score
- **Foreign Keys**: vehicle_id, date

#### 3. fact_fuel_consumption
- **Purpose**: Fuel consumption tracking
- **Measures**: fuel_consumed, cost, efficiency
- **Foreign Keys**: vehicle_id, date

#### 4. fact_inventory
- **Purpose**: Inventory levels and movements
- **Measures**: quantity_on_hand, quantity_ordered, quantity_sold
- **Foreign Keys**: product_id, warehouse_id, date

## KPI Calculations

### Revenue KPIs
- **Daily Revenue**: Total revenue by day
- **Monthly Revenue**: Total revenue by month
- **Average Order Value**: Revenue per order
- **Order Count**: Number of orders

### Performance KPIs
- **Fuel Efficiency**: Distance per unit of fuel
- **Delivery Time**: Average delivery time
- **Efficiency Score**: Overall performance score
- **Distance Traveled**: Total distance covered

### Inventory KPIs
- **Turnover Ratio**: Sales to inventory ratio
- **Stock Levels**: Current inventory levels
- **Reorder Points**: Inventory reorder thresholds

### Supply Chain KPIs
- **On-Time Delivery Rate**: Percentage of on-time deliveries
- **Fulfillment Rate**: Percentage of fulfilled orders
- **Cost Efficiency Ratio**: Revenue to cost ratio

## Analytics Views

### 1. revenue_analytics
- **Purpose**: Revenue analysis with time dimensions
- **Use Case**: Power BI dashboards, revenue reporting
- **Dimensions**: Time, Customer, Product
- **Measures**: Revenue, Order Count, Average Order Value

### 2. performance_analytics
- **Purpose**: Vehicle performance analysis
- **Use Case**: Fleet management, performance monitoring
- **Dimensions**: Time, Vehicle, Geography
- **Measures**: Efficiency, Fuel Consumption, Delivery Time

### 3. inventory_analytics
- **Purpose**: Inventory turnover and stock analysis
- **Use Case**: Inventory management, stock optimization
- **Dimensions**: Time, Product, Warehouse
- **Measures**: Turnover Ratio, Stock Levels, Reorder Points

## Installation

1. **Install Python Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Verify Azure Connection**:
   The connection string and SAS token are already configured in the code.

## Usage

### Option 1: Full Gold Layer Processing

**Using Python directly**:
```bash
cd medallion/3-gold
python gold_layer_launcher.py
```

**Using Windows batch file**:
```bash
cd medallion/3-gold
run_gold_layer.bat
```

**Using root launcher**:
```bash
python run_gold_layer.py
# or
run_gold_layer.bat
```

### Option 2: Modular Processing

**Create dimensions only**:
```bash
cd medallion/3-gold
python gold_layer_launcher.py --dimensions-only
# or
create_dimensions_only.bat
```

**Create facts only**:
```bash
cd medallion/3-gold
python gold_layer_launcher.py --facts-only
# or
create_facts_only.bat
```

**Calculate KPIs only**:
```bash
cd medallion/3-gold
python gold_layer_launcher.py --kpis-only
# or
calculate_kpis_only.bat
```

### Option 3: Check Processing Status

**Using Python directly**:
```bash
cd medallion/3-gold
python gold_layer_launcher.py --status
```

**Using Windows batch file**:
```bash
cd medallion/3-gold
check_gold_status.bat
```

### Option 4: Advanced Usage

```bash
# Custom container names
python gold_layer_launcher.py --silver-container my-silver --gold-container my-gold

# Status check only
python gold_layer_launcher.py --status
```

## Azure Configuration

### Storage Account Details
- **Storage Account**: adls4aldress
- **Silver Container**: silver (source)
- **Gold Container**: gold (destination)
- **Connection**: Pre-configured with SAS token
- **Permissions**: Read from silver, Write to gold

### Container Structure
Gold layer organizes data by type:
```
gold/
├── dimensions/
│   ├── dim_customer.csv
│   ├── dim_product.csv
│   ├── dim_vehicle.csv
│   ├── dim_supplier.csv
│   ├── dim_warehouse.csv
│   ├── dim_geography.csv
│   └── dim_time.csv
├── facts/
│   ├── fact_orders.csv
│   ├── fact_performance.csv
│   ├── fact_fuel_consumption.csv
│   └── fact_inventory.csv
├── kpis/
│   ├── daily_revenue.csv
│   ├── monthly_revenue.csv
│   ├── vehicle_performance.csv
│   ├── inventory_turnover.csv
│   └── supply_chain_metrics.csv
└── analytics/
    ├── revenue_analytics.csv
    ├── performance_analytics.csv
    └── inventory_analytics.csv
```

## Data Quality Features

### Quality Checks
- **Referential Integrity**: Validates foreign key relationships
- **Data Completeness**: Ensures required fields are populated
- **Data Consistency**: Validates data across related tables
- **Surrogate Keys**: Generates unique keys for dimensions

### Quality Metrics
- **Processing Statistics**: Detailed processing metrics
- **Data Lineage**: Complete source-to-destination tracking
- **Error Reporting**: Comprehensive error logging
- **Performance Metrics**: Processing time and throughput

## Monitoring and Logging

### Log Files
- `gold_layer_processing.log` - Processing activities and transformations
- `gold_layer_metadata.json` - Processing metadata and file tracking

### Log Levels
- **INFO**: Normal operations, transformations, statistics
- **WARNING**: Data quality issues, missing relationships
- **ERROR**: Processing failures, Azure connection issues

## Error Handling

### Common Issues and Solutions

1. **Azure Connection Failed**
   - Check internet connectivity
   - Verify SAS token is still valid
   - Check Azure storage account status

2. **Data Processing Failed**
   - Check data format in silver layer
   - Verify file encoding (UTF-8)
   - Review error logs for specific issues

3. **Memory Issues**
   - Large datasets are processed in chunks
   - Consider file size limits
   - Monitor system resources

## Performance Considerations

### Optimization Features
- **Incremental Processing**: Only processes new/changed files
- **Efficient Data Types**: Optimizes memory usage
- **Streaming Processing**: Handles large files efficiently
- **Connection Reuse**: Reuses Azure connections

### Resource Usage
- **Memory**: Optimized for large datasets
- **CPU**: Efficient data transformations
- **Network**: Optimized Azure transfers
- **Storage**: Minimal local storage requirements

## Security

### Data Protection
- **SAS Token**: Secure access to Azure storage
- **HTTPS**: All communications encrypted
- **No Local Storage**: No sensitive data stored locally

### Access Control
- **Read Access**: Can read from silver container
- **Write Access**: Can write to gold container
- **Metadata Access**: Can read/write processing metadata

## Integration with Analytics Tools

### Power BI Integration
1. **Connect to Azure Data Lake Storage Gen2**
2. **Import dimension and fact tables**
3. **Create relationships between tables**
4. **Build dashboards using pre-calculated KPIs**

### Databricks Integration
1. **Mount Azure Data Lake Storage Gen2**
2. **Read gold layer tables as DataFrames**
3. **Perform advanced analytics and ML**
4. **Write results back to gold layer**

### SQL Analytics
1. **Use Azure Synapse Analytics**
2. **Create external tables pointing to gold layer**
3. **Run complex SQL queries**
4. **Build data warehouses**

## Configuration Options

### Dimensional Modeling Settings
```json
{
  "dimensional_modeling": {
    "create_dimensions": true,
    "create_facts": true,
    "create_time_dimension": true,
    "add_surrogate_keys": true
  }
}
```

### KPI Configuration
```json
{
  "kpis": {
    "revenue_kpis": {
      "enabled": true,
      "metrics": ["daily_revenue", "monthly_revenue", "avg_order_value"]
    },
    "performance_kpis": {
      "enabled": true,
      "metrics": ["fuel_efficiency", "delivery_time", "efficiency_score"]
    }
  }
}
```

## Troubleshooting

### Check Processing Status
```python
from gold_layer_processor import GoldLayerProcessor

processor = GoldLayerProcessor(connection_string, "silver", "gold")
status = processor.get_processing_status()
print(status)
```

### Manual Table Creation
```python
from gold_layer_processor import GoldLayerProcessor

processor = GoldLayerProcessor(connection_string, "silver", "gold")
dimensions = processor._create_dimension_tables()
facts = processor._create_fact_tables()
kpis = processor._calculate_kpis()
```

### Reset Processing Metadata
Delete `gold_layer_metadata.json` to force re-processing of all data.

## Support and Maintenance

### Regular Maintenance
- Monitor processing logs for errors
- Check Azure storage usage
- Verify SAS token expiration (expires: 2025-10-06)
- Review data quality metrics

### Monitoring Recommendations
- Set up Azure alerts for storage quota
- Monitor processing success rates
- Track data quality metrics
- Review transformation logs regularly

## Future Enhancements

Potential improvements for future versions:
- **Advanced Analytics**: Machine learning models
- **Real-time Processing**: Stream processing capabilities
- **Data Lineage Visualization**: Visual data lineage tracking
- **Custom KPIs**: User-defined KPI calculations
- **Data Catalog Integration**: Integration with data catalog systems
- **Automated Testing**: Data quality testing framework
- **Performance Optimization**: Query optimization and indexing
