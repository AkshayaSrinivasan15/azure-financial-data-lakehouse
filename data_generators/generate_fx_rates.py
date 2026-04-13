import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

random.seed(42)
np.random.seed(42)

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Simulate 2 years of daily FX rates with realistic drift + volatility
BASE_RATES = {"INR": 82.0, "GBP": 0.80, "EUR": 0.93, "SGD": 1.34, "AED": 3.67, "AUD": 1.54}
VOLATILITY = {"INR": 0.003, "GBP": 0.004, "EUR": 0.003, "SGD": 0.002, "AED": 0.0001, "AUD": 0.005}

start_date = datetime(2023, 1, 1)
end_date   = datetime(2024, 12, 31)
dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

rows = []
current_rates = dict(BASE_RATES)

for dt in dates:
    if dt.weekday() >= 5:   # Skip weekends — FX markets closed
        continue
    for currency, base in BASE_RATES.items():
        # Geometric Brownian Motion for realistic rate movement
        drift = 0.00005
        shock = np.random.normal(0, VOLATILITY[currency])
        current_rates[currency] = round(current_rates[currency] * np.exp(drift + shock), 4)
        rows.append({
            "rate_date":        dt.strftime("%Y-%m-%d"),
            "base_currency":    "USD",
            "quote_currency":   currency,
            "exchange_rate":    current_rates[currency],
            "open_rate":        round(current_rates[currency] * random.uniform(0.998, 1.002), 4),
            "high_rate":        round(current_rates[currency] * random.uniform(1.001, 1.005), 4),
            "low_rate":         round(current_rates[currency] * random.uniform(0.995, 0.999), 4),
            "source":           "ECB",
            "ingestion_ts":     datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

df = pd.DataFrame(rows)
df.to_csv(f"{OUTPUT_DIR}/fx_rates_raw.csv", index=False)
print(f"✅ fx_rates_raw.csv — {len(df)} rows | {df['rate_date'].nunique()} trading days | {df['quote_currency'].nunique()} currencies")
print(df.groupby("quote_currency")["exchange_rate"].agg(["min","max","mean"]).round(4).to_string())
