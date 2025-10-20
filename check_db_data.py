"""
Quick test script to check database dates for NIFTY
"""
from sqlalchemy import create_engine, text
import pandas as pd
from urllib.parse import quote_plus

db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'

db_password_enc = quote_plus(db_password)
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}')

print("Checking TBL_NIFTY_DERIVED table...")

# Check available dates
query1 = """
SELECT DISTINCT "BizDt"
FROM "TBL_NIFTY_DERIVED"
ORDER BY "BizDt" DESC
LIMIT 10
"""
dates = pd.read_sql(text(query1), engine)
print("\n📅 Last 10 dates available:")
print(dates)

# Check expiry dates
query2 = """
SELECT DISTINCT "FininstrmActlXpryDt"
FROM "TBL_NIFTY_DERIVED"
WHERE "FininstrmActlXpryDt" IS NOT NULL
ORDER BY "FininstrmActlXpryDt"
LIMIT 10
"""
expiries = pd.read_sql(text(query2), engine)
print("\n📆 First 10 expiry dates:")
print(expiries)

# Check sample data for latest date
latest_date = dates.iloc[0]['BizDt']
query3 = f"""
SELECT COUNT(*) as total_rows,
       COUNT(DISTINCT "StrkPric") as unique_strikes,
       COUNT(DISTINCT "FininstrmActlXpryDt") as unique_expiries
FROM "TBL_NIFTY_DERIVED"
WHERE "BizDt" = '{latest_date}'
AND "OptnTp" IN ('CE', 'PE')
"""
stats = pd.read_sql(text(query3), engine)
print(f"\n📊 Data for {latest_date}:")
print(stats)

print("\n✅ Check complete!")
