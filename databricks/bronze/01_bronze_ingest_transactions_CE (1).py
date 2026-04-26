# Databricks notebook source
# ============================================================
# BRONZE LAYER — Unity Catalog Volume Version (FINAL FIX)
# ============================================================

# COMMAND ----------

# %md ## 0. Config

BATCH_DATE  = "2024-01-15"
ENV         = "dev"

# ✅ Unity Catalog Volume path (THIS IS THE FIX)
BASE_PATH   = "/Volumes/workspace/default/rawdata"

BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"
GOLD_PATH   = f"{BASE_PATH}/gold"

print(f"✅ Config ready | Batch: {BATCH_DATE}")
print(f"   BRONZE: {BRONZE_PATH}")

# COMMAND ----------

# %md ## 1. Check Uploaded Files (from Volume)

print("Checking uploaded files...")

files = dbutils.fs.ls(BASE_PATH)

for f in files:
    print(f"   {f.name}")

# COMMAND ----------

# %md ## 2. Stage Files into Bronze Structure

year, month, day = BATCH_DATE.split("-")

FILES_TO_STAGE = {
    f"{BASE_PATH}/transactions_raw.csv": f"{BRONZE_PATH}/transactions/transactions_{BATCH_DATE}.csv",
    f"{BASE_PATH}/customers_raw.csv":    f"{BRONZE_PATH}/customers/customers_{BATCH_DATE}.csv",
    f"{BASE_PATH}/products_raw.csv":     f"{BRONZE_PATH}/products/products_{BATCH_DATE}.csv",
    f"{BASE_PATH}/fx_rates_raw.csv":     f"{BRONZE_PATH}/fx_rates/fx_rates_{BATCH_DATE}.csv",
}

for src, dst in FILES_TO_STAGE.items():
    try:
        dbutils.fs.cp(src, dst)
        print(f"✅ Staged: {dst}")
    except Exception as e:
        print(f"⚠️ Failed: {src} → {e}")

# COMMAND ----------

# %md ## 3. Read Transactions CSV

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, current_timestamp

raw_path = f"{BRONZE_PATH}/transactions/transactions_{BATCH_DATE}.csv"

print(f"Reading: {raw_path}")

raw_df = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .csv(raw_path)
)

print(f"Rows: {raw_df.count()}")
raw_df.printSchema()

# COMMAND ----------

# %md ## 4. Validation (Good vs Bad)

if "_corrupt_record" not in raw_df.columns:
    raw_df = raw_df.withColumn("_corrupt_record", lit(None))

good_df = raw_df.filter(
    col("_corrupt_record").isNull() &
    col("transaction_id").isNotNull() &
    col("customer_id").isNotNull() &
    col("amount_usd").isNotNull()
)

bad_df = raw_df.filter(
    col("_corrupt_record").isNotNull() |
    col("transaction_id").isNull() |
    col("customer_id").isNull() |
    col("amount_usd").isNull()
)

print(f"✅ Good rows: {good_df.count()}")
print(f"⚠️ Bad rows : {bad_df.count()}")

# COMMAND ----------

# %md ## 5. Add Audit Columns

bronze_df = (
    good_df
    .drop("_corrupt_record")
    .withColumn("bronze_ingestion_ts", current_timestamp())
    .withColumn("bronze_batch_date", lit(BATCH_DATE))
    .withColumn("year",  lit(year))
    .withColumn("month", lit(month))
    .withColumn("day",   lit(day))
)

# COMMAND ----------

# %md ## 6. Write Delta (Bronze)

bronze_delta_path = f"{BRONZE_PATH}/delta/transactions"

(
    bronze_df
    .write
    .format("delta")
    .mode("append")
    .partitionBy("year","month","day")
    .save(bronze_delta_path)
)

print(f"✅ Written to Delta: {bronze_delta_path}")

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

#spark.sql(f"""
#CREATE TABLE IF NOT EXISTS bronze.transactions
#USING DELTA
#LOCATION '{bronze_delta_path}'
#""")

bronze_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("workspace.default.bronze_transactions")

# COMMAND ----------

# %md ## 7. Quarantine Bad Rows

if bad_df.count() > 0:
    quarantine_path = f"{BRONZE_PATH}/quarantine/transactions_{BATCH_DATE}"

    (
        bad_df
        .withColumn("quarantine_reason", lit("Invalid data"))
        .write
        .format("delta")
        .mode("append")
        .save(quarantine_path)
    )

    print(f"⚠️ Quarantined data → {quarantine_path}")
else:
    print("✅ No bad data")

# COMMAND ----------

# %md ## 8. Ingest Other Tables

from pyspark.sql.functions import current_timestamp

def ingest_simple(name):

    path = f"{BRONZE_PATH}/{name}/{name}_{BATCH_DATE}.csv"

    df = (
        spark.read
        .option("header", "true")
        .csv(path)
        .withColumn("bronze_ingestion_ts", current_timestamp())
    )

    table_name = f"workspace.default.bronze_{name}"

    # ✅ Correct way (Unity Catalog)
    df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(table_name)

    print(f"✅ {table_name}: {df.count()} rows")

ingest_simple("customers")
ingest_simple("products")
ingest_simple("fx_rates")

# COMMAND ----------

# %md ## DONE

print("="*50)
print("🔥 BRONZE LAYER SUCCESS")
print("="*50)