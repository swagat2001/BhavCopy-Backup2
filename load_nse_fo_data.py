import pandas as pd
import glob
import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus


# Database configuration
db_name = 'BhavCopy_Database'
db_user = 'postgres'
# db_password = 'Gallop@3104'  # ← CHANGE THIS
db_host = 'localhost'
db_port = '5432'

# Create database connection
db_password = quote_plus('Gallop@3104')
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Path to CSV files
csv_folder = r'C:\NSE_EOD_FO'
column_names = ['SYMBOL', 'DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']

csv_files = glob.glob(os.path.join(csv_folder, '*.csv'))
print(f"📂 Found {len(csv_files)} CSV file(s)\n")

for csv_file in csv_files:
    try:
        print(f"📄 {os.path.basename(csv_file)}")
        df = pd.read_csv(csv_file, header=None, names=column_names)
        symbols = df['SYMBOL'].unique()
        
        for symbol in symbols:
            symbol_data = df[df['SYMBOL'] == symbol].copy()
            table_name = symbol.replace('-', '_').lower()
            symbol_data.to_sql(table_name, engine, if_exists='append', index=False)
        
        print(f"✅ Loaded {len(symbols)} symbols\n")
    except Exception as e:
        print(f"❌ Error: {str(e)}\n")

print("🎉 Complete!")
