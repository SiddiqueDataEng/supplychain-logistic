# Bronze Layer Processor for Azure Data Lake Storage Gen2

## Overview

The Bronze Layer Processor automatically monitors the `data/raw_data/` directory and uploads CSV files to Azure Data Lake Storage Gen2. It provides both one-time scanning and real-time monitoring capabilities.

## Features

- **Automatic File Detection**: Monitors for new, modified, or moved CSV files
- **Change Detection**: Uses file hashing to detect actual changes
- **Azure ADLS Gen2 Integration**: Direct upload to Azure Data Lake Storage Gen2
- **Date Partitioning**: Organizes files by year/month/day for better data management
- **Real-time Monitoring**: Uses file system events for immediate uploads
- **Metadata Tracking**: Maintains local metadata for efficient change detection
- **Error Handling**: Comprehensive error handling and logging
- **CSV Validation**: Validates CSV files before upload

## Architecture

```
Raw Data Directory (data/raw_data/)
    ↓
Bronze Layer Processor
    ↓
Azure ADLS Gen2 (bronze container)
    ↓
Date-partitioned structure:
    year=YYYY/month=MM/day=DD/filename.csv
```

## Files Structure

- `bronze_layer_processor.py` - Core processor with Azure integration
- `bronze_layer_monitor.py` - Real-time file system monitoring
- `bronze_layer_launcher.py` - Command-line interface
- `bronze_config.json` - Configuration file
- `run_bronze_layer.bat` - Windows batch script for one-time scan
- `run_bronze_monitor.bat` - Windows batch script for real-time monitoring

## Installation

1. **Install Python Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Verify Azure Connection**:
   The connection string and SAS token are already configured in the code.

## Usage

### Option 1: One-time Scan (Recommended for Initial Setup)

**Using Python directly**:
```bash
python bronze_layer_launcher.py --mode scan
```

**Using Windows batch file**:
```bash
run_bronze_layer.bat
```

### Option 2: Real-time Monitoring

**Using Python directly**:
```bash
python bronze_layer_launcher.py --mode monitor
```

**Using Windows batch file**:
```bash
run_bronze_monitor.bat
```

### Option 3: Advanced Usage

```bash
# Custom container name
python bronze_layer_launcher.py --mode scan --container my-bronze-container

# Custom scan interval for monitoring
python bronze_layer_launcher.py --mode monitor --interval 30
```

## Azure Configuration

### Connection Details
- **Storage Account**: adls4aldress
- **Container**: bronze (default)
- **Connection String**: Pre-configured with SAS token
- **Permissions**: Read, Write, Delete, List, Add, Create, Update, Process

### Container Structure
Files are organized in Azure with date partitioning:
```
bronze/
├── year=2025/
│   ├── month=01/
│   │   ├── day=05/
│   │   │   ├── orders_20250105_120000.csv
│   │   │   ├── inventory_20250105_120000.csv
│   │   │   └── ...
│   │   └── day=06/
│   │       └── ...
│   └── month=02/
│       └── ...
```

## File Processing

### Supported File Types
- CSV files only (`.csv` extension)
- Files must be valid CSV format
- Empty files are skipped

### Change Detection
- Uses MD5 hash comparison
- Tracks file modifications, creations, and moves
- Skips unchanged files to optimize performance

### Upload Process
1. **File Validation**: Check if file is valid CSV
2. **Hash Calculation**: Calculate MD5 hash for change detection
3. **Azure Upload**: Upload to Azure ADLS Gen2 with metadata
4. **Metadata Update**: Update local tracking metadata

## Monitoring and Logging

### Log Files
- `bronze_layer_processing.log` - Processing activities
- `bronze_layer_monitoring.log` - Real-time monitoring events
- `bronze_layer_metadata.json` - File tracking metadata

### Log Levels
- **INFO**: Normal operations, file uploads
- **WARNING**: Non-critical issues (empty files, etc.)
- **ERROR**: Upload failures, connection issues

## Error Handling

### Common Issues and Solutions

1. **Azure Connection Failed**
   - Check internet connectivity
   - Verify SAS token is still valid
   - Check Azure storage account status

2. **File Upload Failed**
   - Check file permissions
   - Verify file is not locked by another process
   - Check Azure storage quota

3. **Invalid CSV Files**
   - Ensure files have proper CSV format
   - Check for encoding issues
   - Verify file is not corrupted

## Performance Considerations

### Optimization Features
- **Hash-based Change Detection**: Only uploads changed files
- **Batch Processing**: Efficient handling of multiple files
- **Connection Pooling**: Reuses Azure connections
- **Metadata Caching**: Reduces redundant operations

### Resource Usage
- **Memory**: Minimal memory usage for file processing
- **CPU**: Low CPU usage for hash calculations
- **Network**: Efficient uploads with retry logic
- **Storage**: Local metadata file (~1KB per tracked file)

## Security

### Data Protection
- **SAS Token**: Secure access to Azure storage
- **HTTPS**: All communications encrypted
- **Local Metadata**: No sensitive data stored locally

### Access Control
- **Read Access**: Can read existing blobs
- **Write Access**: Can upload new files
- **Delete Access**: Can overwrite existing files
- **List Access**: Can enumerate container contents

## Troubleshooting

### Check Status
```python
from bronze_layer_processor import BronzeLayerProcessor

processor = BronzeLayerProcessor(connection_string, "bronze")
status = processor.get_upload_status()
print(status)
```

### Manual File Upload
```python
from bronze_layer_processor import BronzeLayerProcessor
from pathlib import Path

processor = BronzeLayerProcessor(connection_string, "bronze")
file_path = Path("data/raw_data/your_file.csv")
success = processor._upload_file_to_azure(file_path)
print(f"Upload successful: {success}")
```

### Reset Metadata
Delete `bronze_layer_metadata.json` to force re-upload of all files.

## Integration with Existing Pipeline

The Bronze Layer integrates seamlessly with your existing data pipeline:

1. **Raw Data** → Bronze Layer (this processor)
2. **Bronze Layer** → Silver Layer (existing `silver_layer_processor.py`)
3. **Silver Layer** → Gold Layer (existing `gold_layer_processor.py`)

## Support and Maintenance

### Regular Maintenance
- Monitor log files for errors
- Check Azure storage usage
- Verify SAS token expiration (expires: 2025-10-06)
- Update dependencies as needed

### Monitoring Recommendations
- Set up Azure alerts for storage quota
- Monitor upload success rates
- Track file processing times
- Review error logs regularly

## Future Enhancements

Potential improvements for future versions:
- **Compression**: Compress files before upload
- **Encryption**: Client-side encryption for sensitive data
- **Parallel Uploads**: Multi-threaded upload processing
- **Web Dashboard**: Web interface for monitoring
- **Email Notifications**: Alert on upload failures
- **Data Quality Checks**: Enhanced CSV validation
