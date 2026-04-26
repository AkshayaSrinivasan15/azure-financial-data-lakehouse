# Databricks notebook source
# File: databricks/gold/04_gold_financial_kpis_CE.py
# ============================================================
# Azure Financial Data Lakehouse Platform
# ============================================================
# GOLD LAYER — FINAL (Community Edition Compatible)
# ============================================================

# COMMAND ----------

# %md ## 0. Config

# COMMAND ----------

BATCH_DATE = "2024-01-15"

print(f"✅ Config | Batch: {BATCH_DATE}")

spark.sql("CREATE DATABASE IF NOT EXISTS workspace.default")

# COMMAND ----------

# %md ## 1. Load Silver Tables (Unity Catalog)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, current_timestamp, round as _round,
    sum as _sum, count as _count, avg as _avg,
    max as _max, min as _min, countDistinct,
    when, coalesce
)

# ✅ Read from Unity Catalog tables (NO DBFS)

txn_df = (
    spark.read.table("workspace.default.silver_transactions")
    .filter(col("silver_batch_date") == BATCH_DATE)
    .filter(col("status_clean") != "UNKNOWN")
)

cust_df = (
    spark.read.table("workspace.default.silver_customers")
    .filter(col("is_current") == 1)
)

prod_df = (
    spark.read.table("workspace.default.bronze_products")
)

print(f"✅ Transactions: {txn_df.count()}")
print(f"✅ Customers: {cust_df.count()}")
print(f"✅ Products: {prod_df.count()}")

# COMMAND ----------

# %md ## 2. Build Fact Table

# COMMAND ----------

txn_df = txn_df.withColumnRenamed("country", "txn_country")
fact_df = (
    txn_df
    .join(cust_df.alias("cust"), on="customer_id", how="left")
    .join(prod_df.alias("prod"), on="product_id", how="left")
    .select(
        "transaction_id",
        "customer_id",
        "product_id",

        col("amount_usd").cast("double"),
        col("amount_usd_fx_adjusted").cast("double"),
        col("fee_charged").cast("double"),
        col("net_amount_usd").cast("double"),

        col("transaction_date"),
        col("transaction_ts"),
        col("txn_year"),
        col("txn_month"),
        col("txn_hour"),

        col("currency"),
        col("channel"),
        col("status_clean"),

        col("is_completed"),
        col("is_failed"),
        col("is_international"),
        col("amount_band"),
        col("risk_flag"),
        col("anomaly_score"),

        coalesce(col("cust.segment"), lit("Unknown")).alias("customer_segment"),
        coalesce(col("cust.risk_rating"), lit("Unknown")).alias("customer_risk_rating"),

        # ✅ ONLY FIX NEEDED
        #coalesce(col("cust.country"), col("country")).alias("customer_country"),
        coalesce(col("cust.country"), col("txn_country")).alias("customer_country"),

        coalesce(col("prod.category"), col("product_category")).alias("product_category"),

        lit(BATCH_DATE).alias("gold_batch_date"),
        current_timestamp().alias("gold_processed_ts")
    )
)

# COMMAND ----------

# %md ## 3. Write Fact Table

# COMMAND ----------

table_name = "workspace.default.gold_fact_transactions"

spark.sql(f"DROP TABLE IF EXISTS {table_name}")

(
    fact_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table_name)
)

print(f"✅ {table_name} created")

# COMMAND ----------

# %md ## 4. Daily KPI Aggregation

# COMMAND ----------

daily_agg = (
    fact_df
    .groupBy("transaction_date","txn_year","txn_month","customer_segment","product_category")
    .agg(
        _count("transaction_id").alias("total_transactions"),
        _sum("is_completed").alias("completed_count"),
        _sum("is_failed").alias("failed_count"),
        _sum(col("amount_usd")).alias("total_volume_usd"),
        _avg(col("amount_usd")).alias("avg_txn_value"),
        countDistinct("customer_id").alias("unique_customers")
    )
    .withColumn("success_rate",
        _round(col("completed_count") / col("total_transactions") * 100, 2)
    )
    .withColumn("gold_batch_date", lit(BATCH_DATE))
    .withColumn("gold_processed_ts", current_timestamp())
)

# COMMAND ----------

# %md ## 5. Write Daily KPI

# COMMAND ----------

table_name = "workspace.default.gold_daily_kpi"

spark.sql(f"DROP TABLE IF EXISTS {table_name}")

(
    daily_agg.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table_name)
)

print(f"✅ {table_name} created")

# COMMAND ----------

# %md ## 6. Customer 360

# COMMAND ----------

fact_df = fact_df.withColumn(
    "is_suspicious",
    when(
        (col("risk_flag") == 1) | (col("amount_usd") > 100000),
        1
    ).otherwise(0)
)
customer_360 = (
    fact_df
    .groupBy("customer_id","customer_segment","customer_risk_rating","customer_country")
    .agg(
        _count("transaction_id").alias("total_txns"),
        _sum(col("amount_usd")).alias("total_volume"),
        _avg(col("amount_usd")).alias("avg_txn"),
        _max(col("transaction_date")).alias("last_txn_date"),
        _sum("is_suspicious").alias("suspicious_count")
    )
    .withColumn("risk_level",
        when(col("total_volume") > 100000, "HIGH")
        .when(col("total_volume") > 50000, "MEDIUM")
        .otherwise("LOW")
    )
    .withColumn("gold_batch_date", lit(BATCH_DATE))
    .withColumn("gold_processed_ts", current_timestamp())
)

# COMMAND ----------

# %md ## 7. Write Customer 360

# COMMAND ----------

table_name = "workspace.default.gold_customer_360"

spark.sql(f"DROP TABLE IF EXISTS {table_name}")

(
    customer_360.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table_name)
)

print(f"✅ {table_name} created")

# COMMAND ----------

# %md ## 8. Risk Dashboard

# COMMAND ----------

risk_df = (
    fact_df
    .groupBy("transaction_date","customer_country","product_category")
    .agg(
        _count("transaction_id").alias("total_txns"),
        _sum("is_suspicious").alias("flagged_txns"),
        _sum(col("amount_usd")).alias("total_volume"),
        _avg(col("anomaly_score")).alias("avg_risk_score")
    )
    .withColumn("risk_rate",
        _round(col("flagged_txns") / col("total_txns") * 100, 2)
    )
    .withColumn("risk_level",
        when(col("risk_rate") > 10, "CRITICAL")
        .when(col("risk_rate") > 5, "HIGH")
        .otherwise("LOW")
    )
    .withColumn("gold_batch_date", lit(BATCH_DATE))
    .withColumn("gold_processed_ts", current_timestamp())
)

# COMMAND ----------

# %md ## 9. Write Risk Dashboard

# COMMAND ----------

table_name = "workspace.default.gold_risk_dashboard"

spark.sql(f"DROP TABLE IF EXISTS {table_name}")

(
    risk_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table_name)
)

print(f"✅ {table_name} created")

# COMMAND ----------

# %md ## 10. Final Summary

# COMMAND ----------

print("="*60)
print("🔥 GOLD LAYER SUCCESS")
print("="*60)

tables = [
    "workspace.default.gold_fact_transactions",
    "workspace.default.gold_daily_kpi",
    "workspace.default.gold_customer_360",
    "workspace.default.gold_risk_dashboard"
]

for t in tables:
    cnt = spark.read.table(t).count()
    print(f"{t} → {cnt} rows")

print("="*60)