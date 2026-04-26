# Databricks notebook source
# ============================================================
# SILVER LAYER — CUSTOMERS SCD TYPE 2 (FINAL CE VERSION)
# ============================================================

# COMMAND ----------

# %md ## 0. Config

BATCH_DATE = "2024-01-15"
TABLE_NAME = "workspace.default.silver_customers"

print(f"✅ Batch: {BATCH_DATE}")

# COMMAND ----------

# %md ## 1. Read Bronze Customers

from pyspark.sql import functions as F
from pyspark.sql.functions import *
from delta.tables import DeltaTable

bronze_customers = (
    spark.read.table("workspace.default.bronze_customers")
    #.filter(col("bronze_batch_date") == BATCH_DATE)
    #.filter(col("bronze_ingestion_ts").isNotNull())
)

print(f"✅ Bronze rows: {bronze_customers.count()}")
display(bronze_customers.limit(5))

# COMMAND ----------

# %md ## 2. PII Masking + Cleaning

def mask_email(email_col):
    local  = split(email_col, "@")[0]
    domain = split(email_col, "@")[1]
    return concat(substring(local,1,1), lit("***"), lit("@"),
                  substring(domain,1,1), lit("***.com"))

def mask_phone(phone_col):
    return concat(lit("****"), substring(phone_col, -4, 4))

def tokenize_dob(dob_col, id_col):
    return sha2(concat_ws("|", dob_col, id_col), 256)

masked_df = (
    bronze_customers
    .withColumn("email", mask_email(col("email")))
    .withColumn("phone", mask_phone(col("phone")))
    .withColumn("dob_token", tokenize_dob(col("date_of_birth"), col("customer_id")))
    .drop("date_of_birth")

    .withColumn("segment", trim(col("segment")))
    .withColumn("risk_rating", trim(col("risk_rating")))
    .withColumn("kyc_status", trim(col("kyc_status")))
    .withColumn("country", trim(col("country")))

    .withColumn("credit_score", col("credit_score").cast("int"))
    .withColumn("annual_income_usd", col("annual_income_usd").cast("double"))
    .withColumn("is_active", col("is_active").cast("int"))

    .withColumn("change_hash", sha2(concat_ws("|",
        col("segment"), col("risk_rating"), col("kyc_status"),
        col("credit_score").cast("string"),
        col("annual_income_usd").cast("string"),
        col("is_active").cast("string")
    ), 256))
)

print("✅ Masking complete")
display(masked_df.limit(5))

# COMMAND ----------

# %md ## 3. Add SCD Columns

incoming_df = (
    masked_df
    .withColumn("scd_start_date", to_date(lit(BATCH_DATE)))
    .withColumn("scd_end_date", lit("9999-12-31").cast("date"))
    .withColumn("is_current", lit(1))
    .withColumn("scd_version", lit(1))
    .withColumn("silver_processed_ts", current_timestamp())
    .withColumn("silver_batch_date", lit(BATCH_DATE))
)

print("✅ SCD columns added")

# COMMAND ----------

# %md ## 4. SCD Type 2 Logic

if spark.catalog.tableExists(TABLE_NAME):

    print("🔁 Existing table → Running SCD2 logic")

    silver_existing = spark.read.table(TABLE_NAME).filter(col("is_current") == 1)

    new_df = incoming_df.alias("src") \
        .join(silver_existing.alias("tgt"), "customer_id", "left_anti")

    changed_df = incoming_df.alias("src") \
        .join(
            silver_existing.select("customer_id","change_hash").alias("tgt"),
            "customer_id"
        ) \
        .filter(col("src.change_hash") != col("tgt.change_hash")) \
        .select("src.*")

    print(f"New: {new_df.count()} | Changed: {changed_df.count()}")

    if changed_df.count() > 0:
        ids = [r.customer_id for r in changed_df.select("customer_id").collect()]
        ids_str = "', '".join(ids)

        spark.sql(f"""
            UPDATE {TABLE_NAME}
            SET is_current = 0,
                scd_end_date = DATE('{BATCH_DATE}')
            WHERE customer_id IN ('{ids_str}')
            AND is_current = 1
        """)

        print("✅ Old records expired")

    final_df = new_df.unionByName(
        changed_df.withColumn("scd_version", lit(2))
    )

    if final_df.count() > 0:
        final_df.write.format("delta").mode("append").saveAsTable(TABLE_NAME)
        print("✅ Inserted new/changed records")
    else:
        print("ℹ️ No changes")

else:
    print("🆕 First run → Creating table")

    incoming_df.write.format("delta").mode("overwrite").saveAsTable(TABLE_NAME)

# COMMAND ----------

# %md ## 5. Validation

display(spark.sql(f"""
SELECT customer_id, risk_rating, is_current,
       scd_start_date, scd_end_date, scd_version
FROM {TABLE_NAME}
LIMIT 10
"""))

total = spark.sql(f"SELECT COUNT(*) FROM {TABLE_NAME}").collect()[0][0]
current = spark.sql(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE is_current = 1").collect()[0][0]

print("="*50)
print("🔥 SCD2 SUCCESS")
print("="*50)
print(f"Total records   : {total}")
print(f"Current records : {current}")
print("="*50)