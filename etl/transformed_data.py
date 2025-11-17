import pandas as pd
import os

pd.set_option('display.max_rows', None)


# FILE PATH

CSV_PATH = r"../properties.csv"

if not os.path.exists(CSV_PATH):
    print("ERROR: CSV not found!")
    exit()

print("Reading raw data...")
df = pd.read_csv(CSV_PATH)

print(f"Raw: {df.shape}\n")
print("First few rows (raw):")
print(df.head(10))
print("\n" + "-"*80)


# FIX COLUMN SHIFT (Critical!)

print("Fixing column misalignment due to missing Zip Code...")

# Find rows where 'Sqft' looks like a date (contains '-')
date_like = df['Sqft'].astype(str).str.contains('-', na=False)
print(f"Found {date_like.sum()} misaligned rows")

# Shift columns RIGHT for bad rows
if date_like.any():
    bad_rows = df[date_like].copy()
    # Insert NaN in Zip Code, shift others
    df.loc[date_like, 'Zip Code'] = pd.NA
    df.loc[date_like, 'Price'] = bad_rows['Sqft']
    df.loc[date_like, 'Sqft'] = bad_rows['Price']
    df.loc[date_like, 'Date Listed'] = bad_rows['Date Listed']

print("Column shift fixed!\n")


# CLEAN & TRANSFORM

print("Starting clean transformation...")

# Drop rows with critical missing data
df = df.dropna(subset=['Address', 'Price', 'Sqft'])
print(f"After drop: {len(df)} rows")

# Fix Zip Code
df['Zip Code'] = df['Zip Code'].fillna(0).astype(int).astype(str).str.zfill(5)

# Standardize names
df.columns = ['address', 'city', 'state', 'zip_code', 'price', 'sqft', 'date_listed']

# Convert types SAFELY
df['price'] = pd.to_numeric(df['price'], errors='coerce').astype('Int64')
df['sqft'] = pd.to_numeric(df['sqft'], errors='coerce').astype('Int64')

# Parse date
df['date_listed'] = pd.to_datetime(df['date_listed'], errors='coerce')

# Add derived fields
df['price_per_sqft'] = (df['price'] / df['sqft']).round(2)
df = df.assign(listing_id='MP' + pd.Series(range(1, len(df)+1)).astype(str).str.zfill(6))

# Reorder
df = df[['listing_id', 'address', 'city', 'state', 'zip_code', 'price', 'sqft', 'price_per_sqft', 'date_listed']]

# ------------------------------------------------------------------
# 4. SHOW FINAL CLEAN DATA
# ------------------------------------------------------------------
print("\n" + "="*100)
print("           CLEAN & TRANSFORMED DATA (POSTGRESQL READY)")
print("="*100)
print(df.to_string(index=False))
print("="*100)

# ------------------------------------------------------------------
# 5. SAVE CLEAN CSV
# ------------------------------------------------------------------
OUTPUT = r"data\clean_properties.csv"
os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
df.to_csv(OUTPUT, index=False)
print(f"\nClean data saved → {OUTPUT}")
print("Ready for PostgreSQL load!")