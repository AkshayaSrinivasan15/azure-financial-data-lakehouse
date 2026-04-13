#!/bin/bash
# Run all data generators in correct dependency order
echo "=================================================="
echo "  Azure Financial Data Lakehouse — Data Generators"
echo "=================================================="
echo ""
pip install pandas numpy faker --quiet

echo "[1/4] Generating customers..."
python generate_customers.py

echo ""
echo "[2/4] Generating products..."
python generate_products.py

echo ""
echo "[3/4] Generating transactions (depends on customers + products)..."
python generate_transactions.py

echo ""
echo "[4/4] Generating FX rates..."
python generate_fx_rates.py

echo ""
echo "[5/5] Generating streaming events sample..."
python generate_streaming_events.py

echo ""
echo "=================================================="
echo "  All files in ./output/"
ls -lh output/
echo "=================================================="
