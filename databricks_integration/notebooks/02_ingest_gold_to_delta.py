# Databricks Notebook: 02_ingest_gold_to_delta
# Purpose: Read Gold (CSV) from ADLS Gen2 (mount or ABFSS) and write curated Delta tables

from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime

storage_account = "adls4aldress"
container = "gold"
base_uri_abfss = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
mount_point = "/mnt/gold"

# Helper: pick best base path (prefer mount)

def best_base():
    try:
        paths = [m.mountPoint for m in dbutils.fs.mounts()]
        if mount_point in paths:
            return mount_point
    except Exception:
        pass
    return base_uri_abfss

base = best_base()
print(f"[INFO] Using base path: {base}")

# Helper: read CSV with header and infer schema

def read_csv(path: str):
    return (spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(path))

# Load dimensions (if present)
try:
    dim_vehicle = read_csv(f"{base}/dimensions/dim_vehicle.csv")
    dim_product = read_csv(f"{base}/dimensions/dim_product.csv")
    dim_supplier = read_csv(f"{base}/dimensions/dim_supplier.csv")
    dim_warehouse = read_csv(f"{base}/dimensions/dim_warehouse.csv")
    dim_geography = read_csv(f"{base}/dimensions/dim_geography.csv")
except Exception as e:
    print(f"[WARN] Some dimension files missing: {e}")

# Load facts
fact_orders = read_csv(f"{base}/facts/fact_orders.csv")
fact_performance = read_csv(f"{base}/facts/fact_performance.csv")
fact_fuel = read_csv(f"{base}/facts/fact_fuel_consumption.csv")

# Minimal conforming (example casts/standardization)
for name, df in [
    ("fact_orders", fact_orders),
    ("fact_performance", fact_performance),
    ("fact_fuel_consumption", fact_fuel)
]:
    print(f"[INFO] {name}: {df.count()} rows, {len(df.columns)} cols")

# Write curated Delta tables (Unity Catalog or Hive)

target_db = "analytics_gold"
try:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_db}")
except Exception as e:
    print(f"[WARN] Could not create database {target_db}: {e}")

(fact_orders.write.mode("overwrite").format("delta").saveAsTable(f"{target_db}.fact_orders"))
(fact_performance.write.mode("overwrite").format("delta").saveAsTable(f"{target_db}.fact_performance"))
(fact_fuel.write.mode("overwrite").format("delta").saveAsTable(f"{target_db}.fact_fuel_consumption"))

print(f"[{datetime.now().isoformat()}] Wrote Delta tables to {target_db}")
