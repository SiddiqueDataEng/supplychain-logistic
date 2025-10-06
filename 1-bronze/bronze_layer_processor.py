"""
Bronze Layer Processor for Azure Data Lake Storage Gen2
Automatically monitors raw_data directory and uploads files to Azure ADLS Gen2
"""

import os
import time
import logging
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import ResourceExistsError, AzureError
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bronze_layer_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BronzeLayerProcessor:
    """
    Bronze Layer Processor for Azure Data Lake Storage Gen2
    Handles automatic file monitoring and upload to Azure ADLS Gen2
    """
    
    def __init__(self, connection_string: str, container_name: str = "bronze"):
        """
        Initialize the Bronze Layer Processor
        
        Args:
            connection_string: Azure Storage connection string
            container_name: Name of the container in Azure ADLS Gen2
        """
        self.connection_string = connection_string
        self.container_name = container_name
        self.blob_service_client = None
        self.container_client = None
        self.file_hashes = {}  # Track file hashes for change detection
        # Default raw data path; can be overridden via bronze_config.json
        self.raw_data_path = Path("raw_data")
        try:
            config_path = Path(__file__).with_name("bronze_config.json")
            if config_path.exists():
                with open(config_path, "r") as f:
                    cfg = json.load(f)
                configured_path = cfg.get("monitoring", {}).get("raw_data_path")
                if configured_path:
                    candidate_path = Path(configured_path)
                    if not candidate_path.is_absolute():
                        candidate_path = config_path.parent.joinpath(candidate_path).resolve()
                    self.raw_data_path = candidate_path
        except Exception as e:
            logger.warning(f"Could not load raw_data_path from config: {str(e)}")
        self.metadata_file = "bronze_layer_metadata.json"
        
        # Initialize Azure connection
        self._initialize_azure_connection()
        
        # Load existing metadata
        self._load_metadata()
    
    def _initialize_azure_connection(self):
        """Initialize Azure Storage connection and create container if needed"""
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(
                self.connection_string
            )
            
            # Create container if it doesn't exist
            try:
                self.container_client = self.blob_service_client.create_container(
                    self.container_name
                )
                logger.info(f"Created new container: {self.container_name}")
            except ResourceExistsError:
                self.container_client = self.blob_service_client.get_container_client(
                    self.container_name
                )
                logger.info(f"Using existing container: {self.container_name}")
                
        except Exception as e:
            logger.error(f"Failed to initialize Azure connection: {str(e)}")
            raise
    
    def _load_metadata(self):
        """Load file metadata for change detection"""
        try:
            if os.path.exists(self.metadata_file):
                with open(self.metadata_file, 'r') as f:
                    self.file_hashes = json.load(f)
                logger.info(f"Loaded metadata for {len(self.file_hashes)} files")
        except Exception as e:
            logger.warning(f"Could not load metadata: {str(e)}")
            self.file_hashes = {}
    
    def _save_metadata(self):
        """Save file metadata for change detection"""
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(self.file_hashes, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save metadata: {str(e)}")
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate MD5 hash of a file for change detection"""
        try:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating hash for {file_path}: {str(e)}")
            return ""
    
    def _get_blob_path(self, file_path: Path) -> str:
        """
        Generate blob path with date partitioning
        Format: year=YYYY/month=MM/day=DD/filename
        """
        file_name = file_path.name
        timestamp = datetime.now()
        
        # Extract date from filename if possible (format: name_YYYYMMDD_HHMMSS.csv)
        try:
            parts = file_name.split('_')
            if len(parts) >= 3:
                date_part = parts[-2]  # YYYYMMDD
                year = date_part[:4]
                month = date_part[4:6]
                day = date_part[6:8]
            else:
                year = timestamp.strftime("%Y")
                month = timestamp.strftime("%m")
                day = timestamp.strftime("%d")
        except:
            year = timestamp.strftime("%Y")
            month = timestamp.strftime("%m")
            day = timestamp.strftime("%d")
        
        return f"year={year}/month={month}/day={day}/{file_name}"
    
    def _upload_file_to_azure(self, file_path: Path) -> bool:
        """
        Upload a single file to Azure ADLS Gen2
        
        Args:
            file_path: Path to the file to upload
            
        Returns:
            bool: True if upload successful, False otherwise
        """
        try:
            blob_path = self._get_blob_path(file_path)
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_path
            )
            
            # Upload file with metadata
            with open(file_path, "rb") as data:
                blob_client.upload_blob(
                    data,
                    overwrite=True,
                    metadata={
                        "source_file": str(file_path),
                        "upload_timestamp": datetime.now().isoformat(),
                        "file_size": str(file_path.stat().st_size),
                        "file_hash": self._calculate_file_hash(file_path)
                    }
                )
            
            logger.info(f"Successfully uploaded: {file_path.name} -> {blob_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload {file_path.name}: {str(e)}")
            return False
    
    def _validate_csv_file(self, file_path: Path) -> bool:
        """
        Validate that the file is a proper CSV file
        
        Args:
            file_path: Path to the file to validate
            
        Returns:
            bool: True if valid CSV, False otherwise
        """
        try:
            # Check file extension
            if not file_path.suffix.lower() == '.csv':
                return False
            
            # Try to read the first few rows to validate CSV structure
            df = pd.read_csv(file_path, nrows=5)
            if df.empty:
                logger.warning(f"Empty CSV file: {file_path.name}")
                return False
            
            logger.info(f"Validated CSV file: {file_path.name} ({len(df.columns)} columns)")
            return True
            
        except Exception as e:
            logger.error(f"Invalid CSV file {file_path.name}: {str(e)}")
            return False
    
    def scan_and_upload_files(self) -> Dict[str, int]:
        """
        Scan raw_data directory and upload new/changed files
        
        Returns:
            Dict with upload statistics
        """
        stats = {
            "total_files": 0,
            "new_files": 0,
            "changed_files": 0,
            "skipped_files": 0,
            "failed_uploads": 0
        }
        
        if not self.raw_data_path.exists():
            logger.error(f"Raw data directory not found: {self.raw_data_path}")
            return stats
        
        # Get all CSV files
        csv_files = list(self.raw_data_path.glob("*.csv"))
        stats["total_files"] = len(csv_files)
        
        logger.info(f"Scanning {len(csv_files)} CSV files in {self.raw_data_path}")
        
        for file_path in csv_files:
            try:
                # Validate CSV file
                if not self._validate_csv_file(file_path):
                    stats["skipped_files"] += 1
                    continue
                
                # Calculate current file hash
                current_hash = self._calculate_file_hash(file_path)
                file_key = str(file_path)
                
                # Check if file is new or changed
                if file_key not in self.file_hashes:
                    # New file
                    logger.info(f"New file detected: {file_path.name}")
                    stats["new_files"] += 1
                elif self.file_hashes[file_key] != current_hash:
                    # Changed file
                    logger.info(f"Changed file detected: {file_path.name}")
                    stats["changed_files"] += 1
                else:
                    # Unchanged file
                    stats["skipped_files"] += 1
                    continue
                
                # Upload file
                if self._upload_file_to_azure(file_path):
                    # Update hash in metadata
                    self.file_hashes[file_key] = current_hash
                else:
                    stats["failed_uploads"] += 1
                    
            except Exception as e:
                logger.error(f"Error processing {file_path.name}: {str(e)}")
                stats["failed_uploads"] += 1
        
        # Save updated metadata
        self._save_metadata()
        
        logger.info(f"Upload completed. Stats: {stats}")
        return stats
    
    def monitor_directory(self, check_interval: int = 60):
        """
        Continuously monitor the raw_data directory for changes
        
        Args:
            check_interval: Time interval in seconds between checks
        """
        logger.info(f"Starting directory monitoring (checking every {check_interval} seconds)")
        
        try:
            while True:
                logger.info("Performing directory scan...")
                stats = self.scan_and_upload_files()
                
                if stats["new_files"] > 0 or stats["changed_files"] > 0:
                    logger.info(f"Uploaded {stats['new_files']} new files and {stats['changed_files']} changed files")
                
                time.sleep(check_interval)
                
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        except Exception as e:
            logger.error(f"Error in monitoring loop: {str(e)}")
    
    def get_upload_status(self) -> Dict:
        """
        Get current upload status and statistics
        
        Returns:
            Dict with status information
        """
        try:
            # List blobs in container
            blobs = list(self.container_client.list_blobs())
            
            status = {
                "container_name": self.container_name,
                "total_blobs": len(blobs),
                "local_files_tracked": len(self.file_hashes),
                "last_scan": datetime.now().isoformat(),
                "recent_blobs": []
            }
            
            # Get recent blobs (last 10)
            recent_blobs = sorted(blobs, key=lambda x: x.last_modified, reverse=True)[:10]
            for blob in recent_blobs:
                status["recent_blobs"].append({
                    "name": blob.name,
                    "size": blob.size,
                    "last_modified": blob.last_modified.isoformat(),
                    "metadata": blob.metadata
                })
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting upload status: {str(e)}")
            return {"error": str(e)}

def main():
    """Main function to run the bronze layer processor"""
    
    # Azure Storage connection string
    CONNECTION_STRING = (
        "BlobEndpoint=https://adls4aldress.blob.core.windows.net/;"
        "QueueEndpoint=https://adls4aldress.queue.core.windows.net/;"
        "FileEndpoint=https://adls4aldress.file.core.windows.net/;"
        "TableEndpoint=https://adls4aldress.table.core.windows.net/;"
        "SharedAccessSignature=sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupiytfx&"
        "se=2025-10-06T12:52:45Z&st=2025-10-05T04:37:45Z&spr=https&"
        "sig=EHAifBcmoHCm2SsLy6IVDRbe9RC0pWInKG4fBo8QoYw%3D"
    )
    
    try:
        # Initialize processor
        processor = BronzeLayerProcessor(
            connection_string=CONNECTION_STRING,
            container_name="bronze"
        )
        
        # Perform initial scan and upload
        logger.info("Starting initial scan and upload...")
        stats = processor.scan_and_upload_files()
        
        # Print results
        print("\n" + "="*50)
        print("BRONZE LAYER UPLOAD SUMMARY")
        print("="*50)
        print(f"Total files scanned: {stats['total_files']}")
        print(f"New files uploaded: {stats['new_files']}")
        print(f"Changed files uploaded: {stats['changed_files']}")
        print(f"Skipped files: {stats['skipped_files']}")
        print(f"Failed uploads: {stats['failed_uploads']}")
        print("="*50)
        
        # Get upload status
        status = processor.get_upload_status()
        print(f"\nContainer: {status.get('container_name', 'N/A')}")
        print(f"Total blobs in Azure: {status.get('total_blobs', 0)}")
        print(f"Local files tracked: {status.get('local_files_tracked', 0)}")
        
        # Ask user if they want to start monitoring
        response = input("\nDo you want to start continuous monitoring? (y/n): ").lower()
        if response == 'y':
            processor.monitor_directory(check_interval=60)
        else:
            logger.info("Bronze layer processor completed. Run again to check for new files.")
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
