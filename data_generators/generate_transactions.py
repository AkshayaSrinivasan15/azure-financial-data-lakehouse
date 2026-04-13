import pandas as pd
import numpy as np
import random
import uuid
from datetime import datetime, timedelta
import os

random.seed(42)
np.random.seed(42)

NUM_TRANSACTIONS = 50000
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load reference data
customers = pd.read_csv(f"{OUTPUT_DIR}/customers_raw.csv")
products  = pd.read_csv(f"{OUTPUT_DIR}/products_raw.csv")

CUST_IDS     = customers["customer_id"].tolist()
CUST_SEGMENT = dict(zip(customers["customer_id"], customers["segment"]))
CUST_COUNTRY = dict(zip(customers["customer_id"], customers["country"]))
CUST_RISK    = dict(zip(customers["customer_id"], customers["risk_rating"]))

PROD_IDS  = products["product_id"].tolist()
PROD_MIN  = dict(zip(products["product_id"], products["min_transaction_amount"]))
PROD_MAX  = dict(zip(products["product_id"], products["max_transaction_amount"]))
PROD_FEE  = dict(zip(products["product_id"], products["fee_percentage"]))
PROD_CAT  = dict(zip(products["product_id"], products["category"]))

CHANNELS   = ["Mobile App", "Internet Banking", "Branch", "ATM", "API", "SWIFT"]
CH_WEIGHTS = [0.40, 0.25, 0.15, 0.10, 0.07, 0.03]
STATUSES   = ["Completed", "Completed", "Completed", "Completed", "Failed", "Pending", "Reversed"]
CURRENCIES = ["USD", "INR", "GBP", "EUR", "SGD", "AED", "AUD"]
FX_RATES   = {"USD":1.0, "INR":83.5, "GBP":0.79, "EUR":0.92, "SGD":1.35, "AED":3.67, "AUD":1.55}

def random_ts(start="2023-01-01", end="2024-12-31"):
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    return s + timedelta(seconds=random.randint(0, int((e - s).total_seconds())))

def flag_anomaly(amount, prod_id, cust_risk):
    """Simple anomaly flag for suspicious transactions — used in Gold layer analytics"""
    if amount > PROD_MAX[prod_id] * 0.85 and cust_risk == "High":
        return 1
    if amount > 500000:
        return 1
    return 0

rows = []
for i in range(NUM_TRANSACTIONS):
    cust_id  = random.choice(CUST_IDS)
    prod_id  = random.choice(PROD_IDS)
    segment  = CUST_SEGMENT[cust_id]
    currency = random.choice(CURRENCIES)

    # Segment-based amount scaling
    scale = {"Corporate": 3.0, "Premium": 1.8, "SME": 1.4, "Retail": 1.0}[segment]
    base_min = PROD_MIN[prod_id]
    base_max = PROD_MAX[prod_id]
    amount   = round(random.uniform(base_min, min(base_max, base_max * scale)), 2)
    fee      = round(amount * PROD_FEE[prod_id] / 100, 2)
    amount_usd = round(amount / FX_RATES.get(currency, 1.0), 2)

    ts = random_ts()

    rows.append({
        "transaction_id":    f"TXN{str(i+1).zfill(8)}",
        "customer_id":       cust_id,
        "product_id":        prod_id,
        "product_category":  PROD_CAT[prod_id],
        "transaction_date":  ts.strftime("%Y-%m-%d"),
        "transaction_time":  ts.strftime("%H:%M:%S"),
        "transaction_ts":    ts.strftime("%Y-%m-%d %H:%M:%S"),
        "amount":            amount,
        "currency":          currency,
        "amount_usd":        amount_usd,
        "fee_charged":       fee,
        "net_amount_usd":    round(amount_usd - fee, 2),
        "channel":           random.choices(CHANNELS, weights=CH_WEIGHTS)[0],
        "status":            random.choices(STATUSES)[0],
        "country":           CUST_COUNTRY[cust_id],
        "risk_flag":         flag_anomaly(amount, prod_id, CUST_RISK[cust_id]),
        "is_international":  1 if currency != "USD" else 0,
        "reference_number":  str(uuid.uuid4()).upper()[:16],
        "batch_date":        ts.strftime("%Y-%m-%d"),
        "source_system":     "CORE_BANKING",
        "ingestion_ts":      datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

df = pd.DataFrame(rows)
# Introduce ~2% intentional duplicates for dedup testing in Silver
dupes = df.sample(frac=0.02, random_state=99)
df = pd.concat([df, dupes], ignore_index=True).sample(frac=1, random_state=7).reset_index(drop=True)

df.to_csv(f"{OUTPUT_DIR}/transactions_raw.csv", index=False)
print(f"✅ transactions_raw.csv — {len(df)} rows (incl. ~2% duplicates for Silver dedup testing)")
print(f"   Date range   : {df['transaction_date'].min()} → {df['transaction_date'].max()}")
print(f"   Total USD vol: ${df['amount_usd'].sum():,.0f}")
print(f"   Risk flagged : {df['risk_flag'].sum()} transactions")
print(f"   Statuses     : {df['status'].value_counts().to_dict()}")
