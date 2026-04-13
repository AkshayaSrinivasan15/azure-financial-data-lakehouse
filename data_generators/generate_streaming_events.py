"""
Streaming event simulator — mimics real-time transactions sent to Azure Event Hubs.
In the project this runs locally and pushes to Event Hubs via the azure-eventhub SDK.
For local testing without Event Hubs, it writes NDJSON to a file.
"""
import json
import random
import uuid
from datetime import datetime
import time
import os

random.seed(42)
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load customer and product IDs from batch data
import pandas as pd
customers = pd.read_csv(f"{OUTPUT_DIR}/customers_raw.csv")
products  = pd.read_csv(f"{OUTPUT_DIR}/products_raw.csv")
CUST_IDS  = customers["customer_id"].tolist()
PROD_IDS  = products["product_id"].tolist()
CHANNELS  = ["Mobile App", "API", "Internet Banking"]

def generate_event():
    """Generate a single streaming transaction event"""
    amount = round(random.uniform(10, 50000), 2)
    return {
        "event_id":         str(uuid.uuid4()),
        "event_type":       "TRANSACTION",
        "customer_id":      random.choice(CUST_IDS),
        "product_id":       random.choice(PROD_IDS),
        "amount":           amount,
        "currency":         random.choice(["USD","INR","GBP","EUR"]),
        "channel":          random.choice(CHANNELS),
        "status":           random.choices(["Completed","Failed","Pending"], weights=[0.90,0.06,0.04])[0],
        "risk_score":       round(random.uniform(0, 1), 3),
        "is_flagged":       1 if amount > 45000 else 0,
        "device_id":        f"DEV{random.randint(1000,9999)}",
        "ip_country":       random.choice(["IN","US","GB","DE","SG"]),
        "event_ts":         datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z",
        "partition_key":    random.choice(CUST_IDS[:10])   # partition by top customers
    }

# ── For LOCAL testing: write 1000 events to NDJSON ──────────────────────
print("Generating 1000 streaming events (local simulation)...")
events = [generate_event() for _ in range(1000)]
with open(f"{OUTPUT_DIR}/streaming_events_sample.ndjson", "w") as f:
    for e in events:
        f.write(json.dumps(e) + "\n")

print(f"✅ streaming_events_sample.ndjson — 1000 events")
print(f"   Flagged high-value: {sum(1 for e in events if e['is_flagged'])} events")
print(f"   Failed transactions: {sum(1 for e in events if e['status']=='Failed')} events")

# ── For AZURE Event Hubs (Phase 5 — uncomment when EH is set up) ────────
# from azure.eventhub import EventHubProducerClient, EventData
# CONN_STR = os.environ["EVENT_HUB_CONNECTION_STR"]   # from Key Vault
# HUB_NAME = "evh-finlakehouse-transactions"
# producer = EventHubProducerClient.from_connection_string(CONN_STR, eventhub_name=HUB_NAME)
# print("Streaming to Azure Event Hubs... (Ctrl+C to stop)")
# while True:
#     batch = producer.create_batch()
#     for _ in range(10):
#         batch.add(EventData(json.dumps(generate_event())))
#     producer.send_batch(batch)
#     print(f"  Sent batch at {datetime.utcnow()}")
#     time.sleep(2)
