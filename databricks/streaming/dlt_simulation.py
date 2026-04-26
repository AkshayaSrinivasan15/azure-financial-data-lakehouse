# Databricks notebook source
# COMMAND ----------
# DLT SIMULATION (Community Edition)

from pyspark.sql.functions import *

BASE_PATH = "/Volumes/workspace/default/rawdata"

# ---------- BRONZE ----------
bronze_txn = (
    spark.read.format("csv")
    .option("header", "true")
    .load(f"{BASE_PATH}/transactions_raw.csv")
    .withColumn("dlt_ingestion_ts", current_timestamp())
)

bronze_txn.write.mode("overwrite").saveAsTable("workspace.default.bronze_transactions_live")

# ---------- SILVER ----------
silver_txn = (
    bronze_txn
    .withColumn("amount_usd", col("amount_usd").cast("double"))
    .withColumn("currency", upper(trim(col("currency"))))
    .withColumn("is_suspicious", when(col("risk_flag") == 1, 1).otherwise(0))
)

silver_txn.write.mode("overwrite").saveAsTable("workspace.default.silver_transactions_live")

# ---------- GOLD ----------
gold_kpi = (
    silver_txn
    .groupBy("transaction_date")
    .agg(
        count("*").alias("total_txns"),
        sum("amount_usd").alias("total_volume"),
        sum("is_suspicious").alias("suspicious_count")
    )
)

gold_kpi.write.mode("overwrite").saveAsTable("workspace.default.gold_kpis_live")

print("🔥 DLT SIMULATION SUCCESS")