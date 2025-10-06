"""
Gold Layer Launcher
Command-line interface for gold layer processing
"""

import os
import sys
import argparse
from pathlib import Path
from gold_layer_processor import GoldLayerProcessor
import json

def main():
    """Main launcher function"""
    
    parser = argparse.ArgumentParser(description="Gold Layer Processor for Azure ADLS Gen2")
    parser.add_argument(
        "--silver-container",
        default="silver",
        help="Azure silver container name (default: silver)"
    )
    parser.add_argument(
        "--gold-container",
        default="gold",
        help="Azure gold container name (default: gold)"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show processing status only"
    )
    parser.add_argument(
        "--dimensions-only",
        action="store_true",
        help="Create only dimension tables"
    )
    parser.add_argument(
        "--facts-only",
        action="store_true",
        help="Create only fact tables"
    )
    parser.add_argument(
        "--kpis-only",
        action="store_true",
        help="Calculate only KPIs"
    )
    
    args = parser.parse_args()
    
    # Load connection from config
    config_path = Path(__file__).with_name("gold_config.json")
    if not config_path.exists():
        print(f"Config not found: {config_path}")
        sys.exit(1)
    with open(config_path, "r") as f:
        cfg = json.load(f)
    CONNECTION_STRING = cfg["azure_storage"]["connection_string"]
    if not args.silver_container and "silver_container" in cfg["azure_storage"]:
        args.silver_container = cfg["azure_storage"]["silver_container"]
    if not args.gold_container and "gold_container" in cfg["azure_storage"]:
        args.gold_container = cfg["azure_storage"]["gold_container"]
    
    print("="*60)
    print("GOLD LAYER PROCESSOR FOR AZURE ADLS GEN2")
    print("="*60)
    print(f"Silver Container: {args.silver_container}")
    print(f"Gold Container: {args.gold_container}")
    print("="*60)
    
    try:
        # Initialize processor
        processor = GoldLayerProcessor(
            connection_string=CONNECTION_STRING,
            silver_container=args.silver_container,
            gold_container=args.gold_container
        )
        
        if args.status:
            # Show status only
            status = processor.get_processing_status()
            
            print("\nPROCESSING STATUS:")
            print(f"  Silver container: {status.get('silver_container', 'N/A')}")
            print(f"  Gold container: {status.get('gold_container', 'N/A')}")
            print(f"  Silver files: {status.get('silver_files', 0)}")
            print(f"  Gold files: {status.get('gold_files', 0)}")
            print(f"  Dimensions: {status.get('dimensions', 0)}")
            print(f"  Facts: {status.get('facts', 0)}")
            print(f"  KPIs: {status.get('kpis', 0)}")
            print(f"  Analytics views: {status.get('analytics_views', 0)}")
            
            if status.get('gold_structure'):
                print(f"\nGOLD LAYER STRUCTURE:")
                print(f"  Dimensions: {status['gold_structure']['dimensions']}")
                print(f"  Facts: {status['gold_structure']['facts']}")
                print(f"  KPIs: {status['gold_structure']['kpis']}")
                print(f"  Analytics: {status['gold_structure']['analytics']}")
        else:
            # Process silver to gold
            if args.dimensions_only:
                print("\nCreating dimension tables only...")
                dimensions = processor._create_dimension_tables()
                print(f"Created {len(dimensions)} dimension tables")
                
                # Upload dimensions
                for dim_name, dim_df in dimensions.items():
                    blob_name = f"dimensions/{dim_name}.csv"
                    if processor._upload_dataframe_to_blob(dim_df, blob_name, {"table_type": "dimension"}):
                        print(f"  Uploaded {dim_name}: {len(dim_df)} records")
                
            elif args.facts_only:
                print("\nCreating fact tables only...")
                facts = processor._create_fact_tables()
                print(f"Created {len(facts)} fact tables")
                
                # Upload facts
                for fact_name, fact_df in facts.items():
                    blob_name = f"facts/{fact_name}.csv"
                    if processor._upload_dataframe_to_blob(fact_df, blob_name, {"table_type": "fact"}):
                        print(f"  Uploaded {fact_name}: {len(fact_df)} records")
                
            elif args.kpis_only:
                print("\nCalculating KPIs only...")
                kpis = processor._calculate_kpis()
                print(f"Calculated {len(kpis)} KPI tables")
                
                # Upload KPIs
                for kpi_name, kpi_df in kpis.items():
                    blob_name = f"kpis/{kpi_name}.csv"
                    if processor._upload_dataframe_to_blob(kpi_df, blob_name, {"table_type": "kpi"}):
                        print(f"  Uploaded {kpi_name}: {len(kpi_df)} records")
                
            else:
                # Full processing
                stats = processor.process_silver_to_gold()
                
                print("\nPROCESSING RESULTS:")
                print(f"  Total silver files: {stats['total_silver_files']}")
                print(f"  Dimensions created: {stats['dimensions_created']}")
                print(f"  Facts created: {stats['facts_created']}")
                print(f"  KPIs calculated: {stats['kpis_calculated']}")
                print(f"  Analytics views created: {stats['views_created']}")
                print(f"  Failed operations: {stats['failed_operations']}")
                
                # Show status
                status = processor.get_processing_status()
                print(f"\nAZURE STATUS:")
                print(f"  Silver container: {status.get('silver_container', 'N/A')}")
                print(f"  Gold container: {status.get('gold_container', 'N/A')}")
                print(f"  Silver files: {status.get('silver_files', 0)}")
                print(f"  Gold files: {status.get('gold_files', 0)}")
                print(f"  Dimensions: {status.get('dimensions', 0)}")
                print(f"  Facts: {status.get('facts', 0)}")
                print(f"  KPIs: {status.get('kpis', 0)}")
                print(f"  Analytics views: {status.get('analytics_views', 0)}")
    
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
