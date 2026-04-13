import pandas as pd
import random
import os

random.seed(42)
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

PRODUCT_CATALOG = [
    # (product_id, name, category, sub_category, currency, min_amount, max_amount, fee_pct, risk_level)
    ("PROD001", "Savings Account",         "Deposit",     "Savings",       "USD", 100,    50000,  0.00, "Low"),
    ("PROD002", "Fixed Deposit 1Y",        "Deposit",     "Term Deposit",  "USD", 1000,   500000, 0.00, "Low"),
    ("PROD003", "Fixed Deposit 3Y",        "Deposit",     "Term Deposit",  "USD", 1000,   500000, 0.00, "Low"),
    ("PROD004", "Current Account",         "Deposit",     "Current",       "USD", 500,    100000, 0.00, "Low"),
    ("PROD005", "Personal Loan",           "Lending",     "Consumer",      "USD", 5000,   100000, 1.50, "Medium"),
    ("PROD006", "Home Loan",               "Lending",     "Mortgage",      "USD", 50000,  2000000,0.75, "Medium"),
    ("PROD007", "Business Loan",           "Lending",     "Commercial",    "USD", 10000,  500000, 1.25, "High"),
    ("PROD008", "Credit Card Platinum",    "Cards",       "Credit",        "USD", 100,    50000,  2.50, "Medium"),
    ("PROD009", "Credit Card Gold",        "Cards",       "Credit",        "USD", 100,    25000,  2.00, "Medium"),
    ("PROD010", "Debit Card",              "Cards",       "Debit",         "USD", 10,     10000,  0.00, "Low"),
    ("PROD011", "International Wire",      "Transfers",   "International", "USD", 100,    500000, 0.50, "Medium"),
    ("PROD012", "Domestic Transfer",       "Transfers",   "Domestic",      "USD", 10,     100000, 0.10, "Low"),
    ("PROD013", "Forex Exchange",          "FX",          "Spot",          "USD", 100,    1000000,0.30, "Medium"),
    ("PROD014", "Mutual Fund Growth",      "Investment",  "Equity",        "USD", 500,    500000, 1.00, "High"),
    ("PROD015", "Mutual Fund Debt",        "Investment",  "Debt",          "USD", 500,    500000, 0.50, "Low"),
    ("PROD016", "Insurance Premium",       "Insurance",   "Life",          "USD", 200,    10000,  0.00, "Low"),
    ("PROD017", "Trade Finance LC",        "Trade",       "Letter of Credit","USD",10000, 5000000,0.25, "High"),
    ("PROD018", "Treasury Bill",           "Investment",  "Government",    "USD", 1000,   10000000,0.05,"Low"),
]

rows = []
for p in PRODUCT_CATALOG:
    rows.append({
        "product_id": p[0], "product_name": p[1], "category": p[2],
        "sub_category": p[3], "base_currency": p[4],
        "min_transaction_amount": p[5], "max_transaction_amount": p[6],
        "fee_percentage": p[7], "risk_level": p[8],
        "is_active": 1, "launched_date": "2018-01-01",
        "source_system": "PRODUCT_MASTER"
    })

df = pd.DataFrame(rows)
df.to_csv(f"{OUTPUT_DIR}/products_raw.csv", index=False)
print(f"✅ products_raw.csv — {len(df)} products across {df['category'].nunique()} categories")
print(df[["product_id","product_name","category","fee_percentage","risk_level"]].to_string(index=False))
