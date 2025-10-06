# Databricks Notebook: 01_setup_mount
# Purpose: Configure access to ADLS Gen2 (Gold) using SAS.
# Supports: direct SAS (via widget or inline) or Databricks Secrets fallback.

from datetime import datetime

# CONFIG
storage_account = "adls4aldress"
container = "gold"
scope = "adls-secrets"  # used only if no direct SAS is provided
sas_key = "gold_sas"    # used only if no direct SAS is provided

# Widgets
try:
    dbutils.widgets.text("sas_token", "")               # direct SAS (no leading '?', but we will normalize)
    dbutils.widgets.text("mount_point", "/mnt/gold")    # override mount point if needed
    dbutils.widgets.dropdown("force_remount", "false", ["false", "true"])  # unmount/remount if true
except Exception:
    pass

sas_token = None
mount_point = "/mnt/gold"
force_remount = False

# 1) Try to get SAS directly (preferred when user passes it)
try:
    sas_from_widget = dbutils.widgets.get("sas_token").strip()
    if sas_from_widget:
        sas_token = sas_from_widget
except Exception:
    pass

# Normalize SAS (strip leading '?')
if sas_token and sas_token.startswith("?"):
    sas_token = sas_token[1:]

# Allow override mount point
try:
    mp = dbutils.widgets.get("mount_point").strip()
    if mp:
        mount_point = mp
except Exception:
    pass

# Allow force remount
try:
    force_remount = dbutils.widgets.get("force_remount").lower() == "true"
except Exception:
    pass

# Optional: inline fallback (uncomment and paste if you want to hardcode)
# sas_token = "sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2025-10-30T19:09:30Z&st=2025-10-05T10:54:30Z&spr=https&sig=..."

# 2) If still not found, fall back to secret
if not sas_token:
    try:
        sas_token = dbutils.secrets.get(scope=scope, key=sas_key)
    except Exception as e:
        raise Exception("No SAS provided via widget and secret fallback failed. Set widget 'sas_token' or configure secret.")

# Configure Spark for ABFS Gen2 with SAS (dfs endpoint)
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.{storage_account}.dfs.core.windows.net", sas_token)

# Also configure WASBS (blob endpoint) for mounting
spark.conf.set(f"fs.azure.sas.{container}.{storage_account}.blob.core.windows.net", sas_token)

base_uri_abfss = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
source_wasbs = f"wasbs://{container}@{storage_account}.blob.core.windows.net"

# Handle force remount
try:
    current_mounts = {m.mountPoint for m in dbutils.fs.mounts()}
    if force_remount and mount_point in current_mounts:
        dbutils.fs.unmount(mount_point)
        print(f"[OK] Unmounted existing {mount_point}")
except Exception as e:
    print(f"[WARN] Unmount step: {e}")

# Mount the container using WASBS + SAS (Blob endpoint)
try:
    current_mounts = {m.mountPoint for m in dbutils.fs.mounts()}
    if mount_point not in current_mounts:
        dbutils.fs.mount(
            source=source_wasbs,
            mount_point=mount_point,
            extra_configs={
                f"fs.azure.sas.{container}.{storage_account}.blob.core.windows.net": sas_token
            }
        )
        print(f"[OK] Mounted {source_wasbs} at {mount_point}")
    else:
        print(f"[OK] Already mounted at {mount_point}")
except Exception as e:
    print(f"[WARN] Mount step: {e}")

# Validate: list top-level prefixes from mount and ABFSS
print(f"[{datetime.now().isoformat()}] Listing paths under {mount_point}")
try:
    display(dbutils.fs.ls(mount_point))
except Exception as e:
    print(f"[WARN] Mount listing skipped: {e}")

print(f"[{datetime.now().isoformat()}] Listing paths under {base_uri_abfss}")
try:
    display(dbutils.fs.ls(base_uri_abfss))
except Exception as e:
    print(f"[WARN] ABFSS listing skipped: {e}")

# Optional: create a catalog/schema for analytics outputs (Unity Catalog enabled workspaces only)
try:
    spark.sql("CREATE CATALOG IF NOT EXISTS analytics")
    spark.sql("CREATE SCHEMA IF NOT EXISTS analytics.gold")
    print("[OK] Created/verified analytics.gold schema")
except Exception as e:
    print(f"[WARN] Skipping UC catalog creation: {e}")
