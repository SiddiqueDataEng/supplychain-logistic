"""
Real-time Bronze Layer Monitor for Azure Data Lake Storage Gen2
Uses file system monitoring to automatically upload files when they change
"""

import os
import time
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from bronze_layer_processor import BronzeLayerProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bronze_layer_monitoring.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BronzeLayerEventHandler(FileSystemEventHandler):
    """
    File system event handler for bronze layer processing
    """
    
    def __init__(self, processor: BronzeLayerProcessor):
        """
        Initialize the event handler
        
        Args:
            processor: BronzeLayerProcessor instance
        """
        self.processor = processor
        self.upload_delay = 5  # Wait 5 seconds before uploading to ensure file is complete
        self.pending_uploads = {}  # Track pending uploads
    
    def on_created(self, event):
        """Handle file creation events"""
        if not event.is_directory and event.src_path.endswith('.csv'):
            self._schedule_upload(event.src_path, "created")
    
    def on_modified(self, event):
        """Handle file modification events"""
        if not event.is_directory and event.src_path.endswith('.csv'):
            self._schedule_upload(event.src_path, "modified")
    
    def on_moved(self, event):
        """Handle file move/rename events"""
        if not event.is_directory and event.dest_path.endswith('.csv'):
            self._schedule_upload(event.dest_path, "moved")
    
    def _schedule_upload(self, file_path: str, event_type: str):
        """
        Schedule a file for upload after a delay
        
        Args:
            file_path: Path to the file
            event_type: Type of event (created, modified, moved)
        """
        file_path = Path(file_path)
        
        # Cancel any existing upload for this file
        if str(file_path) in self.pending_uploads:
            self.pending_uploads[str(file_path)].cancel()
        
        # Schedule new upload
        import threading
        timer = threading.Timer(
            self.upload_delay,
            self._upload_file,
            args=[file_path, event_type]
        )
        timer.start()
        
        self.pending_uploads[str(file_path)] = timer
        logger.info(f"Scheduled upload for {file_path.name} (event: {event_type})")
    
    def _upload_file(self, file_path: Path, event_type: str):
        """
        Upload a file to Azure ADLS Gen2
        
        Args:
            file_path: Path to the file
            event_type: Type of event that triggered the upload
        """
        try:
            # Remove from pending uploads
            if str(file_path) in self.pending_uploads:
                del self.pending_uploads[str(file_path)]
            
            # Check if file still exists and is accessible
            if not file_path.exists():
                logger.warning(f"File no longer exists: {file_path}")
                return
            
            # Validate and upload file
            if self.processor._validate_csv_file(file_path):
                success = self.processor._upload_file_to_azure(file_path)
                if success:
                    # Update metadata
                    current_hash = self.processor._calculate_file_hash(file_path)
                    self.processor.file_hashes[str(file_path)] = current_hash
                    self.processor._save_metadata()
                    logger.info(f"Successfully uploaded {file_path.name} (triggered by {event_type})")
                else:
                    logger.error(f"Failed to upload {file_path.name}")
            else:
                logger.warning(f"Skipped invalid CSV file: {file_path.name}")
                
        except Exception as e:
            logger.error(f"Error uploading {file_path.name}: {str(e)}")

class BronzeLayerMonitor:
    """
    Real-time monitor for bronze layer file uploads
    """
    
    def __init__(self, connection_string: str, container_name: str = "bronze"):
        """
        Initialize the bronze layer monitor
        
        Args:
            connection_string: Azure Storage connection string
            container_name: Name of the container in Azure ADLS Gen2
        """
        self.processor = BronzeLayerProcessor(connection_string, container_name)
        self.observer = Observer()
        self.raw_data_path = Path("../../data/raw_data")
        self.is_monitoring = False
    
    def start_monitoring(self):
        """Start real-time file system monitoring"""
        if not self.raw_data_path.exists():
            logger.error(f"Raw data directory not found: {self.raw_data_path}")
            return False
        
        try:
            # Create event handler
            event_handler = BronzeLayerEventHandler(self.processor)
            
            # Start monitoring
            self.observer.schedule(
                event_handler,
                str(self.raw_data_path),
                recursive=False
            )
            
            self.observer.start()
            self.is_monitoring = True
            
            logger.info(f"Started monitoring directory: {self.raw_data_path}")
            logger.info("Press Ctrl+C to stop monitoring")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start monitoring: {str(e)}")
            return False
    
    def stop_monitoring(self):
        """Stop file system monitoring"""
        if self.is_monitoring:
            self.observer.stop()
            self.observer.join()
            self.is_monitoring = False
            logger.info("Stopped monitoring")
    
    def run_initial_scan(self):
        """Run initial scan to upload existing files"""
        logger.info("Running initial scan...")
        stats = self.processor.scan_and_upload_files()
        
        print("\n" + "="*50)
        print("INITIAL SCAN RESULTS")
        print("="*50)
        print(f"Total files scanned: {stats['total_files']}")
        print(f"New files uploaded: {stats['new_files']}")
        print(f"Changed files uploaded: {stats['changed_files']}")
        print(f"Skipped files: {stats['skipped_files']}")
        print(f"Failed uploads: {stats['failed_uploads']}")
        print("="*50)
        
        return stats
    
    def get_status(self) -> Dict:
        """Get current monitoring status"""
        return {
            "is_monitoring": self.is_monitoring,
            "monitored_directory": str(self.raw_data_path),
            "container_name": self.processor.container_name,
            "files_tracked": len(self.processor.file_hashes),
            "last_scan": datetime.now().isoformat()
        }

def main():
    """Main function to run the bronze layer monitor"""
    
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
        # Initialize monitor
        monitor = BronzeLayerMonitor(
            connection_string=CONNECTION_STRING,
            container_name="bronze"
        )
        
        # Run initial scan
        monitor.run_initial_scan()
        
        # Start real-time monitoring
        if monitor.start_monitoring():
            try:
                # Keep the monitor running
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Stopping monitor...")
                monitor.stop_monitoring()
        else:
            logger.error("Failed to start monitoring")
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
