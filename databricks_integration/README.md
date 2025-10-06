# Databricks Integration - Runbook

## Prereqs
- Databricks workspace (Azure)
- Databricks CLI installed and configured
- Personal Access Token (PAT)
- SAS token for ADLS Gen2 gold container

## Secrets
Create scope and store SAS token:
```bash
databricks secrets create-scope --scope adls-secrets
databricks secrets put --scope adls-secrets --key gold_sas
# paste SAS token when prompted
```

## Notebooks
1. `01_setup_mount`: configures SAS-based access to `abfss://gold@adls4aldress.dfs.core.windows.net`
2. `02_ingest_gold_to_delta`: reads Gold CSVs and writes Delta tables to `analytics_gold`
3. `03_train_predictive_models`: trains a sample model and logs metrics to `analytics_ml.ml_metrics`

Import these notebooks (Repos or workspace import) ensuring their paths match the job JSON.

## Job
Edit `databricks_integration/jobs/gold_ml_job.json` to match your repo path:
- Replace `/Repos/your-repo` with your actual path (e.g., `/Repos/username/project`)

## Deploy
Using PowerShell:
```powershell
./databricks_integration/deploy/deploy_jobs.ps1 -WorkspaceUrl https://adb-xxx.azuredatabricks.net -Token $env:DATABRICKS_TOKEN -RepoPath "/Repos/username/project" -SasToken "sv=...&sig=..."
```

Or using CLI directly:
```bash
export DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net
export DATABRICKS_TOKEN=***
# optional: set secret
databricks secrets put --scope adls-secrets --key gold_sas
# create/update job
databricks jobs create --json-file databricks_integration/jobs/gold_ml_job.json
```

## Run Order
1) `setup_mount` → 2) `ingest_gold` → 3) `train_models`

## Validation
- In notebooks, run `dbutils.fs.ls("abfss://gold@adls4aldress.dfs.core.windows.net")`
- Query in SQL: `SELECT * FROM analytics_gold.fact_performance LIMIT 10;`
- Check metrics: `SELECT * FROM analytics_ml.ml_metrics ORDER BY timestamp DESC;`

## Notes
- Unity Catalog optional; scripts use a standard Hive metastore by default
- Cluster spec in job JSON uses `Standard_DS3_v2`, tweak as needed
- All reads are CSV from Gold; adjust schema if your files differ
