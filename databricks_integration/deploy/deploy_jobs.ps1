param(
  [string]$WorkspaceUrl,
  [string]$Token,
  [string]$FolderName = "aldrees-gold-ml",
  [string]$Scope = "adls-secrets",
  [string]$SecretKey = "gold_sas",
  [string]$SasToken = ""
)

if (-not $WorkspaceUrl -or -not $Token) { Write-Error "Provide -WorkspaceUrl and -Token"; exit 1 }
$env:DATABRICKS_HOST = $WorkspaceUrl
$env:DATABRICKS_TOKEN = $Token

# Resolve Databricks CLI executable
$DatabricksExe = $null
try { $DatabricksExe = (Get-Command databricks -ErrorAction SilentlyContinue).Source } catch {}
if (-not $DatabricksExe) {
  $cand = "C:\\Python\\Scripts\\databricks.exe"
  if (Test-Path $cand) { $DatabricksExe = $cand }
}
if (-not $DatabricksExe) { Write-Error "Databricks CLI not found. Add it to PATH or install via 'pip install databricks-cli'."; exit 1 }

# Ensure scope and set SAS
try { & $DatabricksExe secrets create-scope --scope $Scope 2>$null } catch {}
if ($SasToken) { & $DatabricksExe secrets put --scope $Scope --key $SecretKey --string-value $SasToken }

# Workspace base path for notebooks
$nbBase = "/Shared/$FolderName"

# Upload notebooks
$localNbRoot = "databricks_integration/notebooks"
$nbList = @("01_setup_mount.py","02_ingest_gold_to_delta.py","03_train_predictive_models.py")
foreach ($nb in $nbList) {
  $name = [System.IO.Path]::GetFileNameWithoutExtension($nb)
  $src = Join-Path $localNbRoot $nb
  $dst = "$nbBase/$name"
  & $DatabricksExe workspace mkdirs --path $nbBase
  & $DatabricksExe workspace import --format SOURCE --language PYTHON --overwrite --path $dst --file $src
  Write-Host "Uploaded: $src -> $dst"
}

# Prepare job JSON with NOTEBOOK_BASE replaced
$jobJsonPath = "databricks_integration/jobs/gold_ml_job.json"
$json = Get-Content $jobJsonPath -Raw
$json = $json.Replace("NOTEBOOK_BASE", $nbBase)
$tmpJob = New-TemporaryFile
Set-Content -Path $tmpJob -Value $json

# Create or reset job
try {
  $job = (& $DatabricksExe jobs create --json-file $tmpJob 2>$null | ConvertFrom-Json)
  if ($job -and $job.job_id) {
    Write-Host "Created job: $($job.job_id)"
  } else { throw "create returned no job_id" }
} catch {
  $existing = (& $DatabricksExe jobs list --output JSON | ConvertFrom-Json)
  $target = $existing.jobs | Where-Object { $_.settings.name -eq "Gold_to_Delta_and_ML" }
  if ($target) {
    & $DatabricksExe jobs reset --job-id $($target.job_id) --json-file $tmpJob
    Write-Host "Reset job: $($target.job_id)"
  } else {
    throw $_
  }
}

Remove-Item $tmpJob -Force
Write-Host "Deployment complete. Notebooks at $nbBase; job 'Gold_to_Delta_and_ML' ready."
