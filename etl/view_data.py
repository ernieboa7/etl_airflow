import pandas as pd
import os


CSV_PATH = r"properties.csv"

print(f"Looking for file at:\n{CSV_PATH}\n")

# CONFIGURE PANDAS TO SHOW ALL ROWS & COLUMNS
pd.set_option('display.max_rows', None)      
pd.set_option('display.max_columns', None)   
pd.set_option('display.width', None)         
pd.set_option('display.max_colwidth', None)  

# READ CSV
print("Reading CSV file...\n")
if not os.path.exists(CSV_PATH):
    print("ERROR: File not found!")
    print("   Please check: properties.csv")
    exit()

df = pd.read_csv(CSV_PATH)

# SHOW FULL DATA
print("="*80)
print("              FULL PROPERTY LISTINGS (ALL 10 RECORDS)")
print("="*80)
print(df.to_string(index=False))  
print("="*80)
print(f"\nTotal Listings: {len(df)}")
print(f"Columns: {list(df.columns)}")