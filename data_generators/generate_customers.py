import pandas as pd
import numpy as np
import random
import uuid
from datetime import datetime, timedelta
import json
import os

random.seed(42)
np.random.seed(42)

# ── Config ──────────────────────────────────────────────────────────────
NUM_CUSTOMERS = 500
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Reference data ───────────────────────────────────────────────────────
COUNTRIES = ["India", "USA", "UK", "Germany", "Singapore", "UAE", "Australia"]
COUNTRY_CODES = {"India": "IN", "USA": "US", "UK": "GB", "Germany": "DE",
                 "Singapore": "SG", "UAE": "AE", "Australia": "AU"}
SEGMENTS = ["Retail", "Premium", "Corporate", "SME"]
SEGMENT_WEIGHTS = [0.50, 0.25, 0.15, 0.10]
RISK_RATINGS = ["Low", "Medium", "High"]
RISK_WEIGHTS = [0.60, 0.30, 0.10]
KYC_STATUSES = ["Verified", "Pending", "Expired"]
KYC_WEIGHTS = [0.85, 0.10, 0.05]

FIRST_NAMES = ["Akshaya","Priya","Ravi","Ananya","Karthik","Deepa","Vijay","Sneha",
               "Arjun","Divya","Rahul","Kavya","Amit","Pooja","Suresh","Nisha",
               "James","Emily","Michael","Sarah","David","Emma","Robert","Olivia",
               "Wei","Mei","Raj","Sunita","Carlos","Sofia","Ahmed","Fatima"]
LAST_NAMES = ["Srinivasan","Kumar","Sharma","Patel","Nair","Reddy","Iyer","Gupta",
              "Smith","Johnson","Williams","Brown","Jones","Davis","Miller","Wilson",
              "Chen","Wang","Singh","Shah","Ali","Khan","Lee","Park","Muller","Becker"]

CITIES_BY_COUNTRY = {
    "India": ["Chennai","Mumbai","Bangalore","Delhi","Hyderabad","Pune","Kolkata"],
    "USA": ["New York","San Francisco","Chicago","Austin","Seattle","Boston"],
    "UK": ["London","Manchester","Birmingham","Edinburgh","Bristol"],
    "Germany": ["Berlin","Munich","Frankfurt","Hamburg","Cologne"],
    "Singapore": ["Singapore"],
    "UAE": ["Dubai","Abu Dhabi","Sharjah"],
    "Australia": ["Sydney","Melbourne","Brisbane","Perth"]
}

def random_date(start_year=2015, end_year=2023):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def mask_email(name):
    return f"{name.lower().replace(' ', '.')}_{random.randint(100,999)}@{'gmail' if random.random()>0.4 else 'yahoo'}.com"

rows = []
for i in range(NUM_CUSTOMERS):
    country = random.choices(COUNTRIES, weights=[0.40,0.20,0.10,0.10,0.08,0.07,0.05])[0]
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    full_name = f"{first} {last}"
    segment = random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS)[0]
    onboard_date = random_date(2015, 2022)
    kyc_date = onboard_date + timedelta(days=random.randint(0, 30))
    kyc_expiry = kyc_date + timedelta(days=random.choice([365, 730, 1095]))

    rows.append({
        "customer_id": f"CUST{str(i+1).zfill(5)}",
        "full_name": full_name,
        "email": mask_email(full_name),                   # PII — will be masked in Silver
        "phone": f"+{random.randint(1,99)}{random.randint(1000000000,9999999999)}",  # PII
        "date_of_birth": random_date(1960, 1998).strftime("%Y-%m-%d"),               # PII
        "country": country,
        "country_code": COUNTRY_CODES[country],
        "city": random.choice(CITIES_BY_COUNTRY[country]),
        "segment": segment,
        "risk_rating": random.choices(RISK_RATINGS, weights=RISK_WEIGHTS)[0],
        "kyc_status": random.choices(KYC_STATUSES, weights=KYC_WEIGHTS)[0],
        "kyc_verified_date": kyc_date.strftime("%Y-%m-%d"),
        "kyc_expiry_date": kyc_expiry.strftime("%Y-%m-%d"),
        "onboarding_date": onboard_date.strftime("%Y-%m-%d"),
        "credit_score": random.randint(300, 850) if segment in ["Premium","Corporate"] else random.randint(300, 750),
        "annual_income_usd": round(random.uniform(20000, 500000), 2) if segment == "Corporate"
                             else round(random.uniform(15000, 150000), 2),
        "is_active": random.choices([1, 0], weights=[0.92, 0.08])[0],
        "created_at": onboard_date.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": (onboard_date + timedelta(days=random.randint(0, 500))).strftime("%Y-%m-%d %H:%M:%S"),
        "source_system": "CRM_SYSTEM"
    })

df = pd.DataFrame(rows)
df.to_csv(f"{OUTPUT_DIR}/customers_raw.csv", index=False)
print(f"✅ customers_raw.csv — {len(df)} rows | PII fields: email, phone, date_of_birth")
print(df[["customer_id","full_name","segment","country","risk_rating","kyc_status"]].head(5).to_string(index=False))
