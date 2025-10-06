# Databricks Notebook: 03_train_predictive_models
# Purpose: Train simple predictive models on curated Delta tables and log metrics

from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

# Parameters
source_db = "analytics_gold"
results_table = "ml_metrics"

# Load a sample dataset: performance for fuel efficiency prediction
perf = spark.table(f"{source_db}.fact_performance")

# Minimal cleanup
cols_needed = [c for c in ["distance_traveled", "fuel_consumed", "delivery_time", "efficiency_score"] if c in perf.columns]
perf = perf.select(*cols_needed).dropna()

if len(cols_needed) < 4:
    print("[WARN] Not all expected columns present; proceeding with available features")

# Define features/label
feature_cols = [c for c in cols_needed if c != "efficiency_score"]
label_col = "efficiency_score" if "efficiency_score" in perf.columns else cols_needed[-1]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
rf = RandomForestRegressor(featuresCol="features", labelCol=label_col, numTrees=100, maxDepth=8, seed=42)

pipeline = Pipeline(stages=[assembler, scaler, rf])

# Train/test split
train, test = perf.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train)

pred = model.transform(test)

# Evaluate
r2 = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="r2").evaluate(pred)
rmse = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse").evaluate(pred)
mae = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="mae").evaluate(pred)

# Log metrics to Delta table
spark.sql("CREATE DATABASE IF NOT EXISTS analytics_ml")
metrics_df = spark.createDataFrame([
    (datetime.now().isoformat(), "rf_efficiency", r2, rmse, mae, len(feature_cols))
], ["timestamp", "model", "r2", "rmse", "mae", "num_features"])

(metrics_df.write.mode("append").format("delta").saveAsTable("analytics_ml.ml_metrics"))

print(f"[{datetime.now().isoformat()}] Trained rf_efficiency model | R2={r2:.3f} RMSE={rmse:.3f} MAE={mae:.3f}")
