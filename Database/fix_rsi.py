# RSI Fix Script - Convert pandas-ta to TA-Lib

import os

def fix_precalculate_data():
    file_path = "C:\\Users\\Admin\\Desktop\\BhavCopy Backup2\\Analysis_Tools\\precalculate_data.py"

    if not os.path.exists(file_path):
        print("Error: precalculate_data.py not found!")
        print("Make sure you run this in the Database folder")
        return

    print("Reading file...")
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Backup
    with open("precalculate_data.py.backup", 'w', encoding='utf-8') as f:
        f.writelines(lines)
    print("Backup created: precalculate_data.py.backup")

    # Process line by line
    new_lines = []
    in_rsi_function = False
    skip_until_next_function = False

    for i, line in enumerate(lines):
        # Skip old RSI function content
        if "def calculate_rsi_for_ticker" in line:
            in_rsi_function = True
            skip_until_next_function = True
            # Add new RSI function
            new_lines.append('def calculate_rsi_for_ticker(table_name, current_date, all_dates, rsi_period=14):\n')
            new_lines.append('    """Calculate RSI using TA-Lib"""\n')
            new_lines.append('    if not TALIB_AVAILABLE:\n')
            new_lines.append('        return None\n')
            new_lines.append('    try:\n')
            new_lines.append('        curr_idx = all_dates.index(current_date)\n')
            new_lines.append('        needed_days = max(40, rsi_period + 10)\n')
            new_lines.append('        end_idx = min(curr_idx + needed_days, len(all_dates))\n')
            new_lines.append('        date_range = all_dates[curr_idx:end_idx]\n')
            new_lines.append('        if len(date_range) < rsi_period:\n')
            new_lines.append('            return None\n')
            new_lines.append("        date_placeholders = ','.join([f\"'{d}\" for d in date_range])\n")
            new_lines.append("        query = f'''SELECT \"BizDt\", \"ClsPric\" FROM \"{table_name}\" WHERE \"BizDt\" IN ({date_placeholders}) AND \"ClsPric\" IS NOT NULL ORDER BY \"BizDt\" ASC'''\n")
            new_lines.append('        df = pd.read_sql(text(query), engine)\n')
            new_lines.append('        if df.empty or len(df) < rsi_period:\n')
            new_lines.append('            return None\n')
            new_lines.append("        df['ClsPric'] = pd.to_numeric(df['ClsPric'], errors='coerce')\n")
            new_lines.append("        df = df.dropna(subset=['ClsPric'])\n")
            new_lines.append('        if len(df) < rsi_period:\n')
            new_lines.append('            return None\n')
            new_lines.append("        prices = np.array(df['ClsPric'].values, dtype=float)\n")
            new_lines.append('        rsi_values = talib.RSI(prices, timeperiod=rsi_period)\n')
            new_lines.append('        current_rsi = rsi_values[-1] if len(rsi_values) > 0 else None\n')
            new_lines.append('        return round(float(current_rsi), 2) if current_rsi and not np.isnan(current_rsi) else None\n')
            new_lines.append('    except Exception as e:\n')
            new_lines.append('        return None\n')
            new_lines.append('\n')
            continue

        # Stop skipping when we hit next function
        if skip_until_next_function and line.startswith('def ') and 'calculate_rsi' not in line:
            skip_until_next_function = False

        if skip_until_next_function:
            continue

        # Replace pandas-ta with talib
        if 'import pandas_ta as ta' in line:
            new_lines.append('    import talib\n')
            continue
        if 'PANDAS_TA_AVAILABLE' in line:
            new_lines.append(line.replace('PANDAS_TA_AVAILABLE', 'TALIB_AVAILABLE'))
            continue
        if 'pandas-ta not installed' in line:
            new_lines.append('    print("⚠️ TA-Lib not installed. RSI calculation will be skipped.")\n')
            continue
        if 'pip install pandas-ta' in line:
            new_lines.append('    print("Install: pip install https://github.com/cgohlke/talib-build/releases/download/v0.4.28/TA_Lib-0.4.28-cp311-cp311-win_amd64.whl")\n')
            continue

        # Add numpy import after datetime import
        if 'from datetime import datetime' in line and 'import numpy' not in ''.join(lines[max(0,i-5):i+5]):
            new_lines.append(line)
            new_lines.append('import numpy as np\n')
            continue

        new_lines.append(line)

    # Write updated file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)

    print("="*60)
    print("SUCCESS! File updated")
    print("="*60)
    print("Run: python update_database.py")

if __name__ == "__main__":
    print("="*60)
    print("RSI Fix Script")
    print("="*60)
    fix_precalculate_data()