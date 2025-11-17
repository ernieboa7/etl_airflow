# etl_to_postgres.py
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import os
import re
from dotenv import load_dotenv
load_dotenv()
# DATABASE ENV VARIABLES
NEON_DB_HOST = os.getenv('NEON_DB_HOST')
NEON_DB_PORT = os.getenv('NEON_DB_PORT')
NEON_DB_NAME = os.getenv('NEON_DB_NAME')
NEON_DB_USER = os.getenv('NEON_DB_USER')
NEON_DB_PASSWORD = os.getenv('NEON_DB_PASSWORD')
NEON_DB_SSLMODE = os.getenv('NEON_DB_SSLMODE')


# DATABASE CONFIG

DB_CONFIG = {
    'host': NEON_DB_HOST,
    'dbname': NEON_DB_NAME,
    'port': NEON_DB_HOST,
    'user': NEON_DB_USER,
    'password': NEON_DB_PASSWORD,
    'sslmode': NEON_DB_SSLMODE
    
    
    
     
}


# FILE PATHS

RAW_CSV = r"../properties.csv"
CLEAN_CSV = r"data\clean_properties.csv"
os.makedirs(os.path.dirname(CLEAN_CSV), exist_ok=True)

# EXTRACT & TRANSFORM

print("Starting ETL: Extract → Transform → Load\n")

if not os.path.exists(RAW_CSV):
    print(f"ERROR: {RAW_CSV} not found!")
    exit()

df = pd.read_csv(RAW_CSV)
print(f"Raw data: {len(df)} rows\n")

# Fix misaligned rows (date in Sqft)
print("Fixing column misalignment...")
date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
bad_mask = df['Sqft'].astype(str).str.fullmatch(date_pattern)
if bad_mask.any():
    bad = df[bad_mask].copy()
    df.loc[bad_mask, 'Zip Code'] = pd.NA
    df.loc[bad_mask, 'Price'] = bad['Sqft']
    df.loc[bad_mask, 'Sqft'] = bad['Price']
    df.loc[bad_mask, 'Date Listed'] = bad['Date Listed']
print(f"Fixed {bad_mask.sum()} misaligned rows\n")

# Clean & Transform
df = df.dropna(subset=['Address'])  # Keep rows with address
df['Zip Code'] = df['Zip Code'].fillna(0).astype(int).astype(str).str.zfill(5)

df.columns = ['address', 'city', 'state', 'zip_code', 'price', 'sqft', 'date_listed']

# SAFELY convert price & sqft
df['price'] = pd.to_numeric(df['price'], errors='coerce')
df['sqft'] = pd.to_numeric(df['sqft'], errors='coerce')

# DROP ROWS WHERE PRICE OR SQFT IS MISSING
print(f"Before drop: {len(df)} rows")
df = df.dropna(subset=['price', 'sqft'])
print(f"After drop: {len(df)} rows\n")

# NOW SAFE TO CONVERT TO INT
df['price'] = df['price'].astype(int)
df['sqft'] = df['sqft'].astype(int)

df['date_listed'] = pd.to_datetime(df['date_listed'], errors='coerce')
df['price_per_sqft'] = (df['price'] / df['sqft']).round(2)

# Add ID
df = df.assign(listing_id='MP' + pd.Series(range(1, len(df)+1)).astype(str).str.zfill(6))

# Reorder
df = df[[
    'listing_id', 'address', 'city', 'state', 'zip_code',
    'price', 'sqft', 'price_per_sqft', 'date_listed'
]]

# Save clean
df.to_csv(CLEAN_CSV, index=False)
print(f"Clean data saved → {CLEAN_CSV}\n")


# 4. CONNECT & LOAD

print("Connecting to PostgreSQL...")
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("Connected!\n")
except Exception as e:
    print(f"Connection failed: {e}")
    exit()

# Create table
cur.execute("""
CREATE TABLE IF NOT EXISTS properties (
    listing_id VARCHAR(10) PRIMARY KEY,
    address TEXT NOT NULL,
    city TEXT NOT NULL,
    state CHAR(2) NOT NULL,
    zip_code CHAR(5) NOT NULL,
    price INTEGER NOT NULL,
    sqft INTEGER NOT NULL,
    price_per_sqft NUMERIC(10,2),
    date_listed DATE
);
""")
conn.commit()
print("Table ready\n")

records = [
    (
        r['listing_id'],
        r['address'],
        r['city'],
        r['state'],
        r['zip_code'],
        r['price'],          
        r['sqft'],            
        round(r['price_per_sqft'], 2) if pd.notna(r['price_per_sqft']) else None,
        r['date_listed'].strftime('%Y-%m-%d') if pd.notna(r['date_listed']) else None
    )
    for _, r in df.iterrows()
]

# UPSERT
upsert = """
INSERT INTO properties VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (listing_id) DO UPDATE SET
    price = EXCLUDED.price,
    sqft = EXCLUDED.sqft,
    price_per_sqft = EXCLUDED.price_per_sqft,
    date_listed = EXCLUDED.date_listed;
"""

print("Loading data...")
execute_batch(cur, upsert, records)
conn.commit()
print(f"Loaded {len(records)} records!\n")


cur.execute("SELECT COUNT(*) FROM properties;")
total = cur.fetchone()[0]
print(f"Database has: {total} rows")

cur.close()
conn.close()

print("\n" + "="*60)
print("ETL COMPLETE! Savannah is LIVE in PostgreSQL")
print("="*60)