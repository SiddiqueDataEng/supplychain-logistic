# Silver Layer Processor for Azure Data Lake Storage Gen2

## Overview

The Silver Layer Processor reads data from the Bronze layer in Azure Data Lake Storage Gen2, applies comprehensive data transformations and cleaning, and stores the processed data in the Silver layer. This layer serves as the foundation for analytics-ready data.

## Features

- **Bronze to Silver Pipeline**: Automated processing from bronze to silver layer
- **Data Transformation**: Comprehensive data cleaning and standardization
- **Azure ADLS Gen2 Integration**: Direct integration with Azure Data Lake Storage Gen2
- **Data Quality**: Built-in data quality checks and validation
- **Metadata Tracking**: Complete processing metadata and lineage
- **Error Handling**: Robust error handling and logging
- **Incremental Processing**: Only processes new or changed files

## Architecture

```
Bronze Layer (Azure ADLS Gen2)
    ↓
Silver Layer Processor
    ↓
Data Transformations:
    - Column name cleaning
    - Data type standardization
    - Duplicate removal
    - Missing value handling
    - Metadata addition
    ↓
Silver Layer (Azure ADLS Gen2)
```

## Files Structure

- `silver_layer_processor.py` - Core processor with transformation logic
- `silver_layer_launcher.py` - Command-line interface
- `silver_config.json` - Configuration file
- `run_silver_layer.bat` - Windows batch script for processing
- `check_silver_status.bat` - Windows batch script for status check

## Data Transformations

### 1. Column Name Cleaning
- Converts to lowercase
- Replaces special characters with underscores
- Removes multiple consecutive underscores
- Ensures valid column names

### 2. Data Type Standardization
- **Numeric Data**: Converts to int64/float64 where appropriate
- **Date/Time Data**: Converts to datetime objects
- **Categorical Data**: Standardizes as string objects
- **Missing Values**: Handles NaN, None, null, empty strings

### 3. Duplicate Removal
- Identifies and removes exact duplicate rows
- Maintains data integrity
- Logs removal statistics

### 4. Missing Value Handling
- **Numeric Columns**: Fills with median values
- **Categorical Columns**: Fills with mode or 'Unknown'
- **Other Types**: Forward fill method
- **Quality Thresholds**: Configurable missing value limits

### 5. Processing Metadata
Adds the following columns to each dataset:
- `_source_file`: Original bronze blob name
- `_processed_timestamp`: Processing timestamp
- `_processing_layer`: Always 'silver'
- `_record_id`: Unique record identifier

## Installation

1. **Install Python Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Verify Azure Connection**:
   The connection string and SAS token are already configured in the code.

## Usage

### Option 1: Process Bronze to Silver

**Using Python directly**:
```bash
cd medallion/2-silver
python silver_layer_launcher.py
```

**Using Windows batch file**:
```bash
cd medallion/2-silver
run_silver_layer.bat
```

**Using root launcher**:
```bash
python run_silver_layer.py
# or
run_silver_layer.bat
```

### Option 2: Check Processing Status

**Using Python directly**:
```bash
cd medallion/2-silver
python silver_layer_launcher.py --status
```

**Using Windows batch file**:
```bash
cd medallion/2-silver
check_silver_status.bat
```

### Option 3: Advanced Usage

```bash
# Custom container names
python silver_layer_launcher.py --bronze-container my-bronze --silver-container my-silver

# Status check only
python silver_layer_launcher.py --status
```

## Azure Configuration

### Storage Account Details
- **Storage Account**: adls4aldress
- **Bronze Container**: bronze
- **Silver Container**: silver
- **Connection**: Pre-configured with SAS token
- **Permissions**: Read from bronze, Write to silver

### Container Structure
Silver files maintain the same date partitioning as bronze:
```
silver/
└── year=2025/
    └── month=10/
        └── day=04/
            ├── fuel_20251004_211516_silver.csv
            ├── fulfillment_20251004_211516_silver.csv
            ├── inventory_20251004_211516_silver.csv
            └── ...
```

## Data Quality Features

### Quality Checks
- **Row Count Validation**: Ensures minimum row thresholds
- **Missing Value Analysis**: Tracks missing value percentages
- **Data Type Validation**: Verifies proper data type conversions
- **Duplicate Detection**: Identifies and reports duplicates

### Quality Metrics
- **Processing Statistics**: Detailed processing metrics
- **Data Lineage**: Complete source-to-destination tracking
- **Error Reporting**: Comprehensive error logging
- **Performance Metrics**: Processing time and throughput

## Monitoring and Logging

### Log Files
- `silver_layer_processing.log` - Processing activities and transformations
- `silver_layer_metadata.json` - Processing metadata and file tracking

### Log Levels
- **INFO**: Normal operations, transformations, statistics
- **WARNING**: Data quality issues, missing values
- **ERROR**: Processing failures, Azure connection issues

## Error Handling

### Common Issues and Solutions

1. **Azure Connection Failed**
   - Check internet connectivity
   - Verify SAS token is still valid
   - Check Azure storage account status

2. **Data Processing Failed**
   - Check data format in bronze layer
   - Verify file encoding (UTF-8)
   - Review error logs for specific issues

3. **Memory Issues**
   - Large files are processed in chunks
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
- **CPU**: Efficient data type conversions
- **Network**: Optimized Azure transfers
- **Storage**: Minimal local storage requirements

## Security

### Data Protection
- **SAS Token**: Secure access to Azure storage
- **HTTPS**: All communications encrypted
- **No Local Storage**: No sensitive data stored locally

### Access Control
- **Read Access**: Can read from bronze container
- **Write Access**: Can write to silver container
- **Metadata Access**: Can read/write processing metadata

## Integration with Data Pipeline

The Silver Layer integrates seamlessly with the medallion architecture:

```
Raw Data → Bronze Layer → Silver Layer → Gold Layer
```

### Pipeline Flow
1. **Bronze Layer** → Raw data with basic validation
2. **Silver Layer** → Cleaned, transformed, analytics-ready data ✅ **THIS LAYER**
3. **Gold Layer** → Business-ready aggregated data

## Configuration Options

### Data Transformation Settings
```json
{
  "data_transformation": {
    "clean_column_names": true,
    "standardize_data_types": true,
    "remove_duplicates": true,
    "handle_missing_values": true,
    "add_processing_metadata": true
  }
}
```

### Missing Value Handling
```json
{
  "missing_value_handling": {
    "numeric_fill_method": "median",
    "categorical_fill_method": "mode",
    "default_fill_value": "Unknown"
  }
}
```

### Data Quality Thresholds
```json
{
  "data_quality": {
    "min_rows_threshold": 1,
    "max_missing_percentage": 50,
    "validate_data_types": true
  }
}
```

## Troubleshooting

### Check Processing Status
```python
from silver_layer_processor import SilverLayerProcessor

processor = SilverLayerProcessor(connection_string, "bronze", "silver")
status = processor.get_processing_status()
print(status)
```

### Manual File Processing
```python
from silver_layer_processor import SilverLayerProcessor

processor = SilverLayerProcessor(connection_string, "bronze", "silver")
stats = processor.process_bronze_to_silver()
print(f"Processed {stats['processed_files']} files")
```

### Reset Processing Metadata
Delete `silver_layer_metadata.json` to force re-processing of all files.

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
- **Advanced Data Quality Rules**: Custom validation rules
- **Data Profiling**: Automatic data profiling and statistics
- **Schema Evolution**: Handle schema changes over time
- **Parallel Processing**: Multi-threaded processing
- **Data Lineage Visualization**: Visual data lineage tracking
- **Custom Transformations**: User-defined transformation functions
- **Data Catalog Integration**: Integration with data catalog systems
