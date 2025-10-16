from sqlalchemy import create_engine, inspect, text
import pandas as pd
from urllib.parse import quote_plus
from datetime import datetime
import numpy as np

try:
    print("Install: pip install https://github.com/cgohlke/talib-build/releases/download/v0.4.28/TA_Lib-0.4.28-cp311-cp311-win_amd64.whl")

    import talib
    TALIB_AVAILABLE = True
except ImportError:
    print("⚠️ TA-Lib not installed. RSI calculation will be skipped.")
    print("Install: pip install https://github.com/cgohlke/talib-build/releases/download/v0.4.28/TA_Lib-0.4.28-cp311-cp311-win_amd64.whl")
    TALIB_AVAILABLE = False

# Database config
db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'
db_password_enc = quote_plus(db_password)
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}')

def get_available_dates():
    try:
        inspector = inspect(engine)
        tables = [t for t in inspector.get_table_names() if t.endswith("_DERIVED")]
        if not tables:
            return []
        sample = next((t for t in ['TBL_NIFTY_DERIVED', 'TBL_BANKNIFTY_DERIVED'] if t in tables), tables[0])
        result = pd.read_sql(text(f'SELECT DISTINCT "BizDt" FROM "{sample}" WHERE "BizDt" IS NOT NULL ORDER BY "BizDt" DESC'), engine)
        return [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) for d in result['BizDt']] if not result.empty else []
    except:
        return []

def get_prev_date(curr, dates):
    try:
        i = dates.index(curr)
        return dates[i+1] if i+1 < len(dates) else None
    except:
        return None

def calculate_rsi_for_ticker(table_name, current_date, all_dates, rsi_period=14):
    """Calculate RSI using TA-Lib"""
    if not TALIB_AVAILABLE:
        return None
    try:
        curr_idx = all_dates.index(current_date)
        needed_days = max(40, rsi_period + 10)
        end_idx = min(curr_idx + needed_days, len(all_dates))
        date_range = all_dates[curr_idx:end_idx]
        if len(date_range) < rsi_period:
            return None
        date_placeholders = ','.join([f"'{d}'" for d in date_range])  # FIXED: Added closing quote
        query = f'''SELECT "BizDt", "ClsPric" FROM "{table_name}" WHERE "BizDt" IN ({date_placeholders}) AND "ClsPric" IS NOT NULL ORDER BY "BizDt" ASC'''
        df = pd.read_sql(text(query), engine)
        if df.empty or len(df) < rsi_period:
            return None
        df['ClsPric'] = pd.to_numeric(df['ClsPric'], errors='coerce')
        df = df.dropna(subset=['ClsPric'])
        if len(df) < rsi_period:
            return None
        prices = np.array(df['ClsPric'].values, dtype=float)
        rsi_values = talib.RSI(prices, timeperiod=rsi_period)
        current_rsi = rsi_values[-1] if len(rsi_values) > 0 else None
        return round(float(current_rsi), 2) if current_rsi and not np.isnan(current_rsi) else None
    except Exception as e:
        return None


def calculate_and_store_data(curr_date, prev_date):
    """Calculate data for a date pair and return results"""
    inspector = inspect(engine)
    tables = [t for t in inspector.get_table_names() if t.endswith("_DERIVED")]
    all_dates = get_available_dates()  # Get all dates for RSI calculation
    
    total_data = []
    otm_data = []
    itm_data = []

    for table in tables:
        try:
            ticker = table.replace("TBL_", "").replace("_DERIVED", "")
            query = f'SELECT "BizDt","TckrSymb","StrkPric","OptnTp","UndrlygPric","delta","vega","strike_diff","TtlTrfVal","OpnIntrst","LastPric","ClsPric" FROM "{table}" WHERE "BizDt" IN (:c,:p) AND "OptnTp" IN (\'CE\',\'PE\') AND "StrkPric" IS NOT NULL'
            df = pd.read_sql(text(query), engine, params={"c": curr_date, "p": prev_date})
            
            if df.empty:
                continue
            
            for col in ['StrkPric','UndrlygPric','delta','vega','strike_diff','TtlTrfVal','OpnIntrst','LastPric','ClsPric']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df['BizDt_str'] = df['BizDt'].astype(str)
            dfc = df[df['BizDt_str'] == curr_date].copy()
            dfp = df[df['BizDt_str'] == prev_date].copy()
            
            if dfc.empty or dfp.empty:
                continue
            
            # Get closing price for current date
            closing_price = dfc['UndrlygPric'].iloc[0] if 'UndrlygPric' in dfc.columns and not dfc.empty else dfc['UndrlygPric'].iloc[0]
            
            # Calculate RSI for this ticker
            rsi_value = calculate_rsi_for_ticker(table, curr_date, all_dates, rsi_period=14)
            
            row_total = {'stock': ticker, 'closing_price': float(closing_price), 'rsi': rsi_value}
            row_otm = {'stock': ticker, 'closing_price': float(closing_price), 'rsi': rsi_value}
            row_itm = {'stock': ticker, 'closing_price': float(closing_price), 'rsi': rsi_value}
            
            # ... (rest of your existing code for delta, vega calculations)
            # I'm keeping all your existing logic below this line unchanged
            
            for opt_type, prefix in [('CE', 'call'), ('PE', 'put')]:
                dc = dfc[dfc['OptnTp']==opt_type].copy()
                dp = dfp[dfp['OptnTp']==opt_type].copy()
                
                if not dc.empty and not dp.empty:
                    dm = pd.merge(dc, dp, on=['TckrSymb','StrkPric'], suffixes=('_c','_p'), how='inner')
                    
                    if not dm.empty:
                        dm['delta_chg'] = dm['delta_c'] - dm['delta_p']
                        dm['vega_chg'] = dm['vega_c'] - dm['vega_p']
                        dm['tradval_chg'] = (dm['OpnIntrst_c'] * dm['LastPric_c']) - (dm['OpnIntrst_p'] * dm['LastPric_p'])
                        dm['moneyness_curr'] = (dm['UndrlygPric_c'] - dm['StrkPric']) / dm['UndrlygPric_c']
                        dm['moneyness_prev'] = (dm['UndrlygPric_p'] - dm['StrkPric']) / dm['UndrlygPric_p']
                        dm['money_chg'] = dm['moneyness_curr'] - dm['moneyness_prev']
                        
                        dv = dm[(dm['delta_c'].notna())&(dm['delta_p'].notna())&(dm['delta_c']!=0)&(dm['delta_p']!=0)].copy()
                        
                        if not dv.empty:
                            # TOTAL calculations
                            idx_max = dv['delta_chg'].idxmax()
                            idx_min = dv['delta_chg'].idxmin()
                            row_total[f'{prefix}_delta_pos_strike'] = f"{dv.loc[idx_max,'StrkPric']:.0f}"
                            row_total[f'{prefix}_delta_pos_pct'] = f"{dv.loc[idx_max,'delta_chg']*100:.2f}"
                            row_total[f'{prefix}_delta_neg_strike'] = f"{dv.loc[idx_min,'StrkPric']:.0f}"
                            row_total[f'{prefix}_delta_neg_pct'] = f"{dv.loc[idx_min,'delta_chg']*100:.2f}"
                            
                            # Vega calculations
                            df_pos = dv[dv['vega_chg'] > 0]
                            idx_max = df_pos['vega_chg'].idxmax() if not df_pos.empty else None
                            if idx_max is not None:
                                row_total[f'{prefix}_vega_pos_strike'] = f"{dv.loc[idx_max,'StrkPric']:.0f}"
                                row_total[f'{prefix}_vega_pos_pct'] = f"{dv.loc[idx_max,'vega_chg']*100:.2f}"
                            else:
                                row_total[f'{prefix}_vega_pos_strike'] = 'N/A'
                                row_total[f'{prefix}_vega_pos_pct'] = '0.00'
                            
                            df_neg = dv[dv['vega_chg'] < 0]
                            idx_min = df_neg['vega_chg'].idxmin() if not df_neg.empty else None
                            if idx_min is not None:
                                row_total[f'{prefix}_vega_neg_strike'] = f"{dv.loc[idx_min,'StrkPric']:.0f}"
                                row_total[f'{prefix}_vega_neg_pct'] = f"{dv.loc[idx_min,'vega_chg']*100:.2f}"
                            else:
                                row_total[f'{prefix}_vega_neg_strike'] = 'N/A'
                                row_total[f'{prefix}_vega_neg_pct'] = '0.00'
                            
                            row_total[f'{prefix}_total_tradval'] = float(dv['tradval_chg'].sum())
                            row_total[f'{prefix}_total_money'] = float(dv['money_chg'].sum())
                            
                            # OTM/ITM calculations (keeping your existing logic)
                            otm_cond = dv['strike_diff_c']<0 if opt_type=='CE' else dv['strike_diff_c']>0
                            dotm = dv[otm_cond]
                            
                            if not dotm.empty:
                                idx_max = dotm['delta_chg'].idxmax()
                                idx_min = dotm['delta_chg'].idxmin()
                                row_otm[f'{prefix}_delta_pos_strike'] = f"{dotm.loc[idx_max,'StrkPric']:.0f}"
                                row_otm[f'{prefix}_delta_pos_pct'] = f"{dotm.loc[idx_max,'delta_chg']*100:.2f}"
                                row_otm[f'{prefix}_delta_neg_strike'] = f"{dotm.loc[idx_min,'StrkPric']:.0f}"
                                row_otm[f'{prefix}_delta_neg_pct'] = f"{dotm.loc[idx_min,'delta_chg']*100:.2f}"
                                
                                df_pos = dotm[dotm['vega_chg'] > 0]
                                idx_max = df_pos['vega_chg'].idxmax() if not df_pos.empty else None
                                if idx_max is not None:
                                    row_otm[f'{prefix}_vega_pos_strike'] = f"{dotm.loc[idx_max,'StrkPric']:.0f}"
                                    row_otm[f'{prefix}_vega_pos_pct'] = f"{dotm.loc[idx_max,'vega_chg']*100:.2f}"
                                else:
                                    row_otm[f'{prefix}_vega_pos_strike'] = 'N/A'
                                    row_otm[f'{prefix}_vega_pos_pct'] = '0.00'
                                
                                df_neg = dotm[dotm['vega_chg'] < 0]
                                idx_min = df_neg['vega_chg'].idxmin() if not df_neg.empty else None
                                if idx_min is not None:
                                    row_otm[f'{prefix}_vega_neg_strike'] = f"{dotm.loc[idx_min,'StrkPric']:.0f}"
                                    row_otm[f'{prefix}_vega_neg_pct'] = f"{dotm.loc[idx_min,'vega_chg']*100:.2f}"
                                else:
                                    row_otm[f'{prefix}_vega_neg_strike'] = 'N/A'
                                    row_otm[f'{prefix}_vega_neg_pct'] = '0.00'
                                
                                row_otm[f'{prefix}_total_tradval'] = float(dotm['tradval_chg'].sum())
                                row_otm[f'{prefix}_total_money'] = float(dotm['money_chg'].sum())
                            else:
                                for metric in ['delta', 'vega']:
                                    for sign in ['pos', 'neg']:
                                        row_otm[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                        row_otm[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                                row_otm[f'{prefix}_total_tradval'] = 0
                                row_otm[f'{prefix}_total_money'] = 0
                            
                            # ITM
                            itm_cond = dv['strike_diff_c']>0 if opt_type=='CE' else dv['strike_diff_c']<0
                            ditm = dv[itm_cond]
                            
                            if not ditm.empty:
                                idx_max = ditm['delta_chg'].idxmax()
                                idx_min = ditm['delta_chg'].idxmin()
                                row_itm[f'{prefix}_delta_pos_strike'] = f"{ditm.loc[idx_max,'StrkPric']:.0f}"
                                row_itm[f'{prefix}_delta_pos_pct'] = f"{ditm.loc[idx_max,'delta_chg']*100:.2f}"
                                row_itm[f'{prefix}_delta_neg_strike'] = f"{ditm.loc[idx_min,'StrkPric']:.0f}"
                                row_itm[f'{prefix}_delta_neg_pct'] = f"{ditm.loc[idx_min,'delta_chg']*100:.2f}"
                                
                                df_pos = ditm[ditm['vega_chg'] > 0]
                                idx_max = df_pos['vega_chg'].idxmax() if not df_pos.empty else None
                                if idx_max is not None:
                                    row_itm[f'{prefix}_vega_pos_strike'] = f"{ditm.loc[idx_max,'StrkPric']:.0f}"
                                    row_itm[f'{prefix}_vega_pos_pct'] = f"{ditm.loc[idx_max,'vega_chg']*100:.2f}"
                                else:
                                    row_itm[f'{prefix}_vega_pos_strike'] = 'N/A'
                                    row_itm[f'{prefix}_vega_pos_pct'] = '0.00'
                                
                                df_neg = ditm[ditm['vega_chg'] < 0]
                                idx_min = df_neg['vega_chg'].idxmin() if not df_neg.empty else None
                                if idx_min is not None:
                                    row_itm[f'{prefix}_vega_neg_strike'] = f"{ditm.loc[idx_min,'StrkPric']:.0f}"
                                    row_itm[f'{prefix}_vega_neg_pct'] = f"{ditm.loc[idx_min,'vega_chg']*100:.2f}"
                                else:
                                    row_itm[f'{prefix}_vega_neg_strike'] = 'N/A'
                                    row_itm[f'{prefix}_vega_neg_pct'] = '0.00'
                                
                                row_itm[f'{prefix}_total_tradval'] = float(ditm['tradval_chg'].sum())
                                row_itm[f'{prefix}_total_money'] = float(ditm['money_chg'].sum())
                            else:
                                for metric in ['delta', 'vega']:
                                    for sign in ['pos', 'neg']:
                                        row_itm[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                        row_itm[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                                row_itm[f'{prefix}_total_tradval'] = 0
                                row_itm[f'{prefix}_total_money'] = 0
                        else:
                            for metric in ['delta', 'vega']:
                                for sign in ['pos', 'neg']:
                                    row_total[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                    row_total[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                                    row_otm[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                    row_otm[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                                    row_itm[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                    row_itm[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                            for row in [row_total, row_otm, row_itm]:
                                row[f'{prefix}_total_tradval'] = 0
                                row[f'{prefix}_total_money'] = 0
                    else:
                        for metric in ['delta', 'vega']:
                            for sign in ['pos', 'neg']:
                                row_total[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                row_total[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                                row_otm[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                row_otm[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                                row_itm[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                row_itm[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                        for row in [row_total, row_otm, row_itm]:
                            row[f'{prefix}_total_tradval'] = 0
                            row[f'{prefix}_total_money'] = 0
                else:
                    for metric in ['delta', 'vega']:
                        for sign in ['pos', 'neg']:
                            row_total[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                            row_total[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                            row_otm[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                            row_otm[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                            row_itm[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                            row_itm[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                    for row in [row_total, row_otm, row_itm]:
                        row[f'{prefix}_total_tradval'] = 0
                        row[f'{prefix}_total_money'] = 0
            
            if len(row_otm) > 2:  # Has stock, closing_price, rsi at minimum
                total_data.append(row_total)
                otm_data.append(row_otm)
                itm_data.append(row_itm)
        except:
            pass
    
    return total_data, otm_data, itm_data

def create_precalculated_tables():
    """Create tables to store pre-calculated results"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS options_dashboard_cache (
        id SERIAL PRIMARY KEY,
        biz_date DATE NOT NULL,
        prev_date DATE NOT NULL,
        moneyness_type VARCHAR(10) NOT NULL,
        data_json TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(biz_date, prev_date, moneyness_type)
    );
    CREATE INDEX IF NOT EXISTS idx_dashboard_cache_dates ON options_dashboard_cache(biz_date, prev_date, moneyness_type);
    """
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
    print("✅ Cache table created")

def precalculate_all_dates():
    """Pre-calculate data for all available date pairs"""
    print("\n" + "="*80)
    print("PRE-CALCULATING DATA FOR ALL DATES")
    print("="*80 + "\n")
    
    dates = get_available_dates()
    if not dates:
        print("❌ No dates found")
        return
    
    print(f"📅 Found {len(dates)} dates\n")
    
    # Check which dates are already calculated
    existing_query = "SELECT DISTINCT biz_date FROM options_dashboard_cache"
    try:
        existing_df = pd.read_sql(text(existing_query), engine)
        existing_dates = set(pd.to_datetime(existing_df['biz_date']).dt.strftime('%Y-%m-%d'))
        print(f"📂 Found {len(existing_dates)} dates already in cache\n")
    except Exception as e:
        existing_dates = set()
        print(f"📂 No existing cache found (starting fresh)\n")
    
    processed = 0
    skipped = 0
    
    for i, curr_date in enumerate(dates[:-1], 1):
        prev_date = get_prev_date(curr_date, dates)
        if not prev_date:
            continue
        
        if curr_date in existing_dates:
            print(f"[{i}/{len(dates)-1}] {curr_date} ... ⏭️  (already exists)")
            skipped += 1
            continue
        
        print(f"[{i}/{len(dates)-1}] {curr_date} ... ", end="", flush=True)
        
        try:
            total_data, otm_data, itm_data = calculate_and_store_data(curr_date, prev_date)
            
            if total_data and otm_data and itm_data:
                # Store in database
                import json
                total_json = json.dumps(total_data)
                otm_json = json.dumps(otm_data)
                itm_json = json.dumps(itm_data)
                
                insert_sql = """
                INSERT INTO options_dashboard_cache (biz_date, prev_date, moneyness_type, data_json)
                VALUES (:curr, :prev, :type, :data)
                ON CONFLICT (biz_date, prev_date, moneyness_type)
                DO UPDATE SET data_json = EXCLUDED.data_json, created_at = CURRENT_TIMESTAMP
                """
                
                with engine.begin() as conn:
                    conn.execute(text(insert_sql), {"curr": curr_date, "prev": prev_date, "type": "TOTAL", "data": total_json})
                    conn.execute(text(insert_sql), {"curr": curr_date, "prev": prev_date, "type": "OTM", "data": otm_json})
                    conn.execute(text(insert_sql), {"curr": curr_date, "prev": prev_date, "type": "ITM", "data": itm_json})
                
                processed += 1
                print(f"✅ ({len(total_data)} tickers)")
            else:
                print("⚠️  (no data)")
        except Exception as e:
            print(f"❌ ({str(e)[:50]})")
    
    print(f"\n" + "="*80)
    print(f"✅ COMPLETE: Processed {processed}, Skipped {skipped}")
    print("="*80 + "\n")

if __name__ == '__main__':
    print("="*80)
    print("OPTIONS DASHBOARD - DATA PRE-CALCULATOR")
    print("="*80)
    print("\nThis script will:")
    print("  1. Create cache table in database")
    print("  2. Calculate data for all available dates")
    print("  3. Calculate RSI (14-period) for each ticker")
    print("  4. Store results in database for fast retrieval")
    print("\n" + "="*80)
    
    if not TALIB_AVAILABLE:
        print("⚠️ TA-Lib not installed. RSI calculation will be skipped.")
        print("RSI calculations will be skipped.")
        print("Install: pip install https://github.com/cgohlke/talib-build/releases/download/v0.4.28/TA_Lib-0.4.28-cp311-cp311-win_amd64.whl")
    
    input("\nPress Enter to start...")
    
    create_precalculated_tables()
    precalculate_all_dates()
    
    print("\n✅ All data pre-calculated and stored!")
    print("📊 Now run dashboard_server.py to use the dashboard")
    input("\nPress Enter to exit...")
