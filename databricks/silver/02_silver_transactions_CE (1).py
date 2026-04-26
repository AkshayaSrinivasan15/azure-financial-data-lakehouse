# Databricks notebook source
# ============================================================
# SILVER LAYER — Unity Catalog Version (FINAL FIX)
# ============================================================

# COMMAND ----------

# COMMAND ----------
# 1. Read Bronze Transactions

from pyspark.sql.functions import *

df = spark.read.table("workspace.default.bronze_transactions")

print("✅ Bronze transactions loaded")
display(df)

# COMMAND ----------

# %md ## 0. Config
from pyspark.sql.functions import *

BATCH_DATE  = "2024-01-15"
ENV         = "dev"

BASE_PATH   = "/Volumes/workspace/default/rawdata"

BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"

# ✅ FIXED TYPE ISSUE
year_val, month_val, day_val = map(int, BATCH_DATE.split("-"))

print(f"✅ Config | Batch: {BATCH_DATE}")

# COMMAND ----------

# %md ## 1. Read Bronze Delta

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp, to_date,
    when, coalesce, upper, trim, round as _round,
    row_number, year, month, dayofmonth, hour
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

bronze_df = (
    spark.read.format("delta")
    .load(f"{BRONZE_PATH}/delta/transactions")
    .filter(
        (col("year")  == year_val) &
        (col("month") == month_val) &
        (col("day")   == day_val)
    )
)

raw_count = bronze_df.count()
print(f"✅ Bronze rows loaded: {raw_count:,}")

# COMMAND ----------

# %md ## 2. Deduplication

dedup_window = Window.partitionBy("transaction_id").orderBy(col("bronze_ingestion_ts").desc())

deduped_df = (
    bronze_df
    .withColumn("row_num", row_number().over(dedup_window))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

deduped_count = deduped_df.count()
dupes_removed = raw_count - deduped_count

dup_pct = (dupes_removed / raw_count * 100) if raw_count > 0 else 0

print(f"✅ After dedup   : {deduped_count:,}")
print(f"   Dupes removed : {dupes_removed:,} ({dup_pct:.2f}%)")

# COMMAND ----------

# %md ## 3. Type Casting

typed_df = (
    deduped_df
    .withColumn("transaction_ts",   to_timestamp("transaction_ts"))
    .withColumn("transaction_date", to_date("transaction_date"))
    .withColumn("amount",           col("amount").cast("double"))
    .withColumn("amount_usd",       col("amount_usd").cast("double"))
    .withColumn("fee_charged",      col("fee_charged").cast("double"))
    .withColumn("net_amount_usd",   col("net_amount_usd").cast("double"))
    .withColumn("risk_flag",        col("risk_flag").cast("int"))
    .withColumn("is_international", col("is_international").cast("int"))

    .withColumn("currency", upper(trim(col("currency"))))
    .withColumn("status_clean",
        when(upper(col("status")).isin("COMPLETED","FAILED","PENDING","REVERSED"),
             upper(col("status")))
        .otherwise("UNKNOWN")
    
    )
)
silver_df = typed_df
print("✅ Type casting complete")

# COMMAND ----------

# COMMAND ----------
# 3. Create Typed Data (fix missing column issue)

typed_df = (
    typed_df
    .withColumn("status_clean", upper(col("status")))
)

print("✅ typed_df created")

# COMMAND ----------

# %md ## 4. FX Enrichment

fx_df = (
    #spark.read.format("delta")
    #.load(f"{BRONZE_PATH}/delta/fx_rates")
    spark.read.table("workspace.default.bronze_fx_rates")
    .select(
        col("quote_currency").alias("fx_currency"),
        col("exchange_rate").cast("double").alias("fx_rate")
    )
    .dropDuplicates(["fx_currency"])
)

#enriched_df = (
    #typed_df
    #.join(fx_df, df["currency"] == fx_df["fx_currency"], "left")
    #.withColumn("fx_rate", coalesce(col("fx_rate"), lit(1.0)))
    #.withColumn("amount_usd_fx_adjusted",
       # round(col("amount") / col("fx_rate"), 2)
    #)
#)

enriched_df = (
    typed_df
    .join(fx_df, typed_df["currency"] == fx_df["fx_currency"], "left")
    .withColumn("fx_rate", coalesce(col("fx_rate"), lit(1.0)))
    .withColumn("amount_usd_fx_adjusted",
        round(col("amount") / col("fx_rate"), 2)
    )
)

print(f"✅ FX enrichment done")

# COMMAND ----------

# %md ## 5. Business Columns

silver_df = (
    enriched_df
    .withColumn("txn_year",  year(col("transaction_date")))
    .withColumn("txn_month", month(col("transaction_date")))
    .withColumn("txn_day",   dayofmonth(col("transaction_date")))
    .withColumn("txn_hour",  hour(col("transaction_ts")))

    .withColumn("amount_band",
        when(col("amount_usd") < 1000, "micro")
        .when(col("amount_usd") < 10000, "small")
        .when(col("amount_usd") < 100000, "medium")
        .otherwise("large")
    )

    .withColumn("anomaly_score",
        col("risk_flag") * 0.5 +
        when(col("amount_band") == "large", 0.4).otherwise(0.0)
    )

    .withColumn("is_completed", when(col("status_clean") == "COMPLETED", 1).otherwise(0))
    .withColumn("is_failed",    when(col("status_clean") == "FAILED", 1).otherwise(0))

    .withColumn("silver_processed_ts", current_timestamp())
    .withColumn("silver_batch_date", lit(BATCH_DATE))
)

print("✅ Business columns ready")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.default.silver_transactions")

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

# %md ## 6. Write Silver (Unity Catalog)

table_name = "workspace.default.silver_transactions"

silver_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(table_name)

print(f"✅ Written to {table_name}")

# COMMAND ----------

# %md ## 7. Validation

null_txn_ids  = silver_df.filter(col("transaction_id").isNull()).count()
negative_amts = silver_df.filter(col("amount_usd") < 0).count()

assert null_txn_ids == 0, "❌ Null transaction IDs found!"
assert negative_amts == 0, "❌ Negative amounts found!"

print("="*50)
print("🔥 SILVER TRANSACTIONS SUCCESS")
print("="*50)