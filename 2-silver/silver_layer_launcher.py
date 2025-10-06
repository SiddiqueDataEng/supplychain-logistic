"""
Silver Layer Launcher
Command-line interface for silver layer processing
"""

import os
import sys
import argparse
from pathlib import Path
from silver_layer_processor import SilverLayerProcessor
import json

def main():
    """Main launcher function"""
    
    parser = argparse.ArgumentParser(description="Silver Layer Processor for Azure ADLS Gen2")
    parser.add_argument(
        "--bronze-container",
        default="bronze",
        help="Azure bronze container name (default: bronze)"
    )
    parser.add_argument(
        "--silver-container",
        default="silver",
        help="Azure silver container name (default: silver)"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show processing status only"
    )
    
    args = parser.parse_args()
    
    # Load connection from config
    config_path = Path(__file__).with_name("silver_config.json")
    if not config_path.exists():
        print(f"Config not found: {config_path}")
        sys.exit(1)
    with open(config_path, "r") as f:
        cfg = json.load(f)
    CONNECTION_STRING = cfg["azure_storage"]["connection_string"]
    if not args.bronze_container and "bronze_container" in cfg["azure_storage"]:
        args.bronze_container = cfg["azure_storage"]["bronze_container"]
    if not args.silver_container and "silver_container" in cfg["azure_storage"]:
        args.silver_container = cfg["azure_storage"]["silver_container"]
    
    print("="*60)
    print("SILVER LAYER PROCESSOR FOR AZURE ADLS GEN2")
    print("="*60)
    print(f"Bronze Container: {args.bronze_container}")
    print(f"Silver Container: {args.silver_container}")
    print("="*60)
    
    try:
        # Initialize processor
        processor = SilverLayerProcessor(
            connection_string=CONNECTION_STRING,
            bronze_container=args.bronze_container,
            silver_container=args.silver_container
        )
        
        if args.status:
            # Show status only
            status = processor.get_processing_status()
            
            print("\nPROCESSING STATUS:")
            print(f"  Bronze container: {status.get('bronze_container', 'N/A')}")
            print(f"  Silver container: {status.get('silver_container', 'N/A')}")
            print(f"  Bronze files: {status.get('bronze_files', 0)}")
            print(f"  Silver files: {status.get('silver_files', 0)}")
            print(f"  Processed files: {status.get('processed_files', 0)}")
            
            if status.get('recent_silver_files'):
                print(f"\nRECENT SILVER FILES:")
                for file_info in status['recent_silver_files'][:5]:
                    print(f"  - {file_info['name']} ({file_info['size']} bytes)")
        else:
            # Process bronze to silver
            stats = processor.process_bronze_to_silver()
            
            print("\nPROCESSING RESULTS:")
            print(f"  Total bronze files: {stats['total_bronze_files']}")
            print(f"  Processed files: {stats['processed_files']}")
            print(f"  Skipped files: {stats['skipped_files']}")
            print(f"  Failed files: {stats['failed_files']}")
            print(f"  Total rows processed: {stats['total_rows_processed']}")
            
            # Show status
            status = processor.get_processing_status()
            print(f"\nAZURE STATUS:")
            print(f"  Bronze container: {status.get('bronze_container', 'N/A')}")
            print(f"  Silver container: {status.get('silver_container', 'N/A')}")
            print(f"  Bronze files: {status.get('bronze_files', 0)}")
            print(f"  Silver files: {status.get('silver_files', 0)}")
            print(f"  Processed files: {status.get('processed_files', 0)}")
    
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
