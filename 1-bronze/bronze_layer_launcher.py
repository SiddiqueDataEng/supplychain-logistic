"""
Bronze Layer Launcher
Simple interface to run bronze layer operations
"""

import os
import sys
import argparse
from pathlib import Path
from bronze_layer_processor import BronzeLayerProcessor
from bronze_layer_monitor import BronzeLayerMonitor
import json

def main():
    """Main launcher function"""
    
    parser = argparse.ArgumentParser(description="Bronze Layer Processor for Azure ADLS Gen2")
    parser.add_argument(
        "--mode",
        choices=["scan", "monitor"],
        default="scan",
        help="Operation mode: scan (one-time) or monitor (continuous)"
    )
    parser.add_argument(
        "--container",
        default="bronze",
        help="Azure container name (default: bronze)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Scan interval in seconds for monitor mode (default: 60)"
    )
    
    args = parser.parse_args()
    
    # Load connection from config
    config_path = Path(__file__).with_name("bronze_config.json")
    if not config_path.exists():
        print(f"Config not found: {config_path}")
        sys.exit(1)
    with open(config_path, "r") as f:
        cfg = json.load(f)
    CONNECTION_STRING = cfg["azure_storage"]["connection_string"]
    if args.container is None and "container_name" in cfg["azure_storage"]:
        args.container = cfg["azure_storage"]["container_name"]
    
    print("="*60)
    print("BRONZE LAYER PROCESSOR FOR AZURE ADLS GEN2")
    print("="*60)
    print(f"Container: {args.container}")
    print(f"Mode: {args.mode}")
    print("="*60)
    
    try:
        if args.mode == "scan":
            # One-time scan mode
            processor = BronzeLayerProcessor(CONNECTION_STRING, args.container)
            stats = processor.scan_and_upload_files()
            
            print("\nSCAN RESULTS:")
            print(f"  Total files: {stats['total_files']}")
            print(f"  New files: {stats['new_files']}")
            print(f"  Changed files: {stats['changed_files']}")
            print(f"  Skipped files: {stats['skipped_files']}")
            print(f"  Failed uploads: {stats['failed_uploads']}")
            
            # Show status
            status = processor.get_upload_status()
            print(f"\nAZURE STATUS:")
            print(f"  Container: {status.get('container_name', 'N/A')}")
            print(f"  Total blobs: {status.get('total_blobs', 0)}")
            print(f"  Local files tracked: {status.get('local_files_tracked', 0)}")
            
        elif args.mode == "monitor":
            # Real-time monitoring mode
            monitor = BronzeLayerMonitor(CONNECTION_STRING, args.container)
            
            # Run initial scan
            print("\nRunning initial scan...")
            monitor.run_initial_scan()
            
            # Start monitoring
            print(f"\nStarting real-time monitoring...")
            print("Press Ctrl+C to stop")
            
            if monitor.start_monitoring():
                try:
                    import time
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    print("\nStopping monitor...")
                    monitor.stop_monitoring()
            else:
                print("Failed to start monitoring")
                sys.exit(1)
    
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
