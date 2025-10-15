import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import re

# === 1. Get file from user ===
filename = input("Enter the filename (with path if needed): ")

# === 2. Load the file ===
if filename.endswith('.csv'):
    df = pd.read_csv(filename)
elif filename.endswith('.xlsx') or filename.endswith('.xls'):
    df = pd.read_excel(filename)
else:
    raise ValueError("Unsupported file format. Please use CSV or Excel.")

# === Convert date columns ===
df['TradDt'] = pd.to_datetime(df['TradDt'], errors='coerce').dt.date
df['XpryDt'] = pd.to_datetime(df['XpryDt'], errors='coerce').dt.date

# === 3. Check required column ===
if "TckrSymb" not in df.columns:
    raise ValueError("Missing required column: 'TckrSymb'")

# === Helper function to sanitize table names ===
def sanitize_table_name(name):
    return re.sub(r'\W+', '_', name).upper()

# === 4. PostgreSQL Connection ===
db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'

db_password_enc = quote_plus(db_password)

engine = create_engine(
    f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}'
)

# === 5. Define Table Schema Template ===
table_schema_template = """
CREATE TABLE IF NOT EXISTS "{table_name}" (
    "BizDt" DATE,
    "Sgmt" VARCHAR(50),
    "FinInstrmTp" VARCHAR(50),
    "TckrSymb" VARCHAR(50),
    "FininstrmActlXpryDt" DATE,
    "StrkPric" VARCHAR(50),
    "OptnTp" VARCHAR(50),
    "FinInstrmNm" VARCHAR(50),
    "OpnPric" VARCHAR(50),
    "HghPric" VARCHAR(50),
    "LwPric" VARCHAR(50),
    "ClsPric" VARCHAR(50),
    "LastPric" VARCHAR(50),
    "PrvsClsgPric" VARCHAR(50),
    "UndrlygPric" VARCHAR(50),
    "SttlmPric" VARCHAR(50),
    "OpnIntrst" VARCHAR(50),
    "ChngInOpnIntrst" VARCHAR(50),
    "TtlTradgVol" VARCHAR(50),
    "TtlTrfVal" VARCHAR(50),
    "TtlNbOfTxsExctd" VARCHAR(50),
    "NewBrdLotQty" VARCHAR(50)
);
"""


# === 6. Get unique ticker symbols ===
unique_symbols = df["TckrSymb"].dropna().unique()

# === 7. Create tables ===
with engine.begin() as conn:
    for symbol in unique_symbols:
        table_name = sanitize_table_name(symbol)
        sql = table_schema_template.format(table_name=table_name)
        print(f"\n🟨 Running SQL:\n{sql}\n")
        try:
            conn.execute(text(sql))
            print(f"✅ Table created (if not exists): {table_name}")
        except Exception as e:
            print(f"❌ Error creating table {table_name}: {e}")

# === 8. Insert rows into respective tables with batching ===
batch_size = 100

for symbol in unique_symbols:
    table_name = sanitize_table_name(symbol)
    df_symbol = df[df["TckrSymb"] == symbol]
    rows_to_insert = df_symbol.to_dict(orient='records')

    if not rows_to_insert:
        continue

    columns = df_symbol.columns.tolist()
    cols_str = ', '.join(f'"{col}"' for col in columns)
    values_str = ', '.join(f":{col}" for col in columns)
    insert_sql = f'INSERT INTO "{table_name}" ({cols_str}) VALUES ({values_str})'

    total_batches = (len(rows_to_insert) + batch_size - 1) // batch_size

    for i in range(total_batches):
        batch = rows_to_insert[i*batch_size:(i+1)*batch_size]
        try:
            with engine.begin() as conn:
                conn.execute(text(insert_sql), batch)
            print(f"✅ Inserted batch {i+1}/{total_batches} ({len(batch)} rows) into {table_name}")
        except Exception as e:
            print(f"❌ Error inserting batch {i+1} into {table_name}: {e}")

print("\n🎉 All tables created and data inserted (where possible) successfully.")
