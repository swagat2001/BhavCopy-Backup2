"""
PRE-CALCULATE DASHBOARD DATA WITH HYBRID RSI(40)
=================================================

HYBRID APPROACH:
1. Try TradingView first (tvDatafeed) for RSI(40) + Price
2. Fallback to Database (TA-Lib) if TradingView fails - RSI(40)
3. All other metrics from Database only
"""

from sqlalchemy import create_engine, inspect, text
import pandas as pd
from urllib.parse import quote_plus
import numpy as np
import json

# Optional imports for HYBRID RSI
try:
    from tvDatafeed import TvDatafeed, Interval
    TVDATAFEED_AVAILABLE = True
    print("✅ TradingView (tvDatafeed) available")
except ImportError:
    TVDATAFEED_AVAILABLE = False
    print("⚠️ TradingView not available (will use database only)")

try:
    import talib
    TALIB_AVAILABLE = True
    print("✅ TA-Lib available")
except ImportError:
    TALIB_AVAILABLE = False
    print("⚠️ TA-Lib not available (RSI fallback disabled)")

# Database config
db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'
db_password_enc = quote_plus(db_password)
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}')

# TradingView ticker mapping
TICKER_MAPPING = {
    'NIFTY': 'NIFTY',
    'BANKNIFTY': 'BANKNIFTY',
    'FINNIFTY': 'FINNIFTY',
    'MIDCPNIFTY': 'MIDCPNIFTY',
    '360ONE': '360ONE',
}

tv_instance = None

def get_tv_instance():
    """Get or create TradingView instance"""
    global tv_instance
    if tv_instance is None and TVDATAFEED_AVAILABLE:
        try:
            tv_instance = TvDatafeed()
            print("✅ TradingView instance created")
        except Exception as e:
            print(f"⚠️ TradingView initialization failed: {str(e)[:50]}")
    return tv_instance

def fetch_tradingview_data(ticker, current_date):
    """
    PRIMARY: Try to get RSI + Price from TradingView
    Returns: (price, rsi) or (None, None)
    """
    if not TVDATAFEED_AVAILABLE:
        return None, None
    
    try:
        tv = get_tv_instance()
        if not tv:
            return None, None
        
        symbol = TICKER_MAPPING.get(ticker, ticker)
        data = tv.get_hist(symbol, 'NSE', interval=Interval.in_daily, n_bars=100, extended_session=False)
        
        if data is None or data.empty:
            return None, None
        
        # Calculate RSI(40) - Wilder's method
        delta = data['close'].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1/40, min_periods=40, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/40, min_periods=40, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, 1e-10)
        data['rsi'] = 100 - (100 / (1 + rs))
        
        data = data.reset_index()
        data['date_str'] = pd.to_datetime(data['datetime']).dt.strftime('%Y-%m-%d')
        matching = data[data['date_str'] == current_date]
        
        if not matching.empty:
            price = float(matching['close'].iloc[0])
            rsi = float(matching['rsi'].iloc[0]) if pd.notna(matching['rsi'].iloc[0]) else None
            if price and rsi:
                return price, rsi
        
        return None, None
    except Exception as e:
        return None, None

def calculate_db_rsi(table_name, current_date, all_dates):
    """
    FALLBACK: Calculate RSI(40) from database using TA-Lib
    Returns: rsi or None
    """
    if not TALIB_AVAILABLE:
        return None
    
    try:
        curr_idx = all_dates.index(current_date)
        end_idx = min(curr_idx + 100, len(all_dates))
        date_range = all_dates[curr_idx:end_idx]
        
        if len(date_range) < 40:
            return None
        
        date_str = ','.join([f"'{d}'" for d in date_range])
        query = f'SELECT "BizDt", "ClsPric" FROM "{table_name}" WHERE "BizDt" IN ({date_str}) AND "ClsPric" IS NOT NULL ORDER BY "BizDt" ASC'
        df = pd.read_sql(text(query), engine)
        
        if df.empty or len(df) < 40:
            return None
        
        df['ClsPric'] = pd.to_numeric(df['ClsPric'], errors='coerce')
        df = df.dropna(subset=['ClsPric'])
        
        if len(df) < 40:
            return None
        
        prices = np.array(df['ClsPric'].values, dtype=float)
        rsi_values = talib.RSI(prices, timeperiod=40)
        current_rsi = rsi_values[-1] if len(rsi_values) > 0 else None
        
        return round(float(current_rsi), 2) if current_rsi and not np.isnan(current_rsi) else None
    except:
        return None

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

def calculate_and_store_data(curr_date, prev_date):
    """Calculate all dashboard data with HYBRID RSI"""
    inspector = inspect(engine)
    tables = [t for t in inspector.get_table_names() if t.endswith("_DERIVED")]
    all_dates = get_available_dates()
    
    total_data, otm_data, itm_data = [], [], []

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
            
            # HYBRID RSI: Try TradingView first, fallback to Database
            tv_price, tv_rsi = fetch_tradingview_data(ticker, curr_date)
            if tv_price and tv_rsi:
                closing_price = tv_price
                rsi_value = tv_rsi
                source = "TV"
            else:
                closing_price = float(dfc['UndrlygPric'].iloc[0] if 'UndrlygPric' in dfc.columns else dfc['ClsPric'].iloc[0])
                rsi_value = calculate_db_rsi(table, curr_date, all_dates)
                source = "DB"
            
            print(f"    {source}: {ticker:15} | Price: {closing_price:10.2f} | RSI: {rsi_value if rsi_value else 'N/A'}")
            
            row_total = {'stock': ticker, 'closing_price': float(closing_price), 'rsi': rsi_value}
            row_otm = {'stock': ticker, 'closing_price': float(closing_price), 'rsi': rsi_value}
            row_itm = {'stock': ticker, 'closing_price': float(closing_price), 'rsi': rsi_value}
            
            # Calculate all other metrics (Delta, Vega, etc.)
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
                            # TOTAL
                            for metric, data_col in [('delta', 'delta_chg'), ('vega', 'vega_chg')]:
                                df_pos = dv[dv[data_col]>0] if metric=='vega' else dv
                                df_neg = dv[dv[data_col]<0] if metric=='vega' else dv
                                
                                if not df_pos.empty:
                                    idx = df_pos[data_col].idxmax()
                                    row_total[f'{prefix}_{metric}_pos_strike'] = f"{dv.loc[idx,'StrkPric']:.0f}"
                                    row_total[f'{prefix}_{metric}_pos_pct'] = f"{dv.loc[idx,data_col]*100:.2f}"
                                else:
                                    row_total[f'{prefix}_{metric}_pos_strike'] = 'N/A'
                                    row_total[f'{prefix}_{metric}_pos_pct'] = '0.00'
                                
                                if not df_neg.empty:
                                    idx = df_neg[data_col].idxmin()
                                    row_total[f'{prefix}_{metric}_neg_strike'] = f"{dv.loc[idx,'StrkPric']:.0f}"
                                    row_total[f'{prefix}_{metric}_neg_pct'] = f"{dv.loc[idx,data_col]*100:.2f}"
                                else:
                                    row_total[f'{prefix}_{metric}_neg_strike'] = 'N/A'
                                    row_total[f'{prefix}_{metric}_neg_pct'] = '0.00'
                            
                            row_total[f'{prefix}_total_tradval'] = float(dv['tradval_chg'].sum())
                            row_total[f'{prefix}_total_money'] = float(dv['money_chg'].sum())
                            
                            # OTM/ITM
                            for cond_type, row_dict in [('OTM', row_otm), ('ITM', row_itm)]:
                                if cond_type == 'OTM':
                                    cond = dv['strike_diff_c']<0 if opt_type=='CE' else dv['strike_diff_c']>0
                                else:
                                    cond = dv['strike_diff_c']>0 if opt_type=='CE' else dv['strike_diff_c']<0
                                
                                dsub = dv[cond]
                                if not dsub.empty:
                                    for metric, data_col in [('delta', 'delta_chg'), ('vega', 'vega_chg')]:
                                        df_pos = dsub[dsub[data_col]>0] if metric=='vega' else dsub
                                        df_neg = dsub[dsub[data_col]<0] if metric=='vega' else dsub
                                        
                                        if not df_pos.empty:
                                            idx = df_pos[data_col].idxmax()
                                            row_dict[f'{prefix}_{metric}_pos_strike'] = f"{dsub.loc[idx,'StrkPric']:.0f}"
                                            row_dict[f'{prefix}_{metric}_pos_pct'] = f"{dsub.loc[idx,data_col]*100:.2f}"
                                        else:
                                            row_dict[f'{prefix}_{metric}_pos_strike'] = 'N/A'
                                            row_dict[f'{prefix}_{metric}_pos_pct'] = '0.00'
                                        
                                        if not df_neg.empty:
                                            idx = df_neg[data_col].idxmin()
                                            row_dict[f'{prefix}_{metric}_neg_strike'] = f"{dsub.loc[idx,'StrkPric']:.0f}"
                                            row_dict[f'{prefix}_{metric}_neg_pct'] = f"{dsub.loc[idx,data_col]*100:.2f}"
                                        else:
                                            row_dict[f'{prefix}_{metric}_neg_strike'] = 'N/A'
                                            row_dict[f'{prefix}_{metric}_neg_pct'] = '0.00'
                                    
                                    row_dict[f'{prefix}_total_tradval'] = float(dsub['tradval_chg'].sum())
                                    row_dict[f'{prefix}_total_money'] = float(dsub['money_chg'].sum())
                                else:
                                    for metric in ['delta', 'vega']:
                                        for sign in ['pos', 'neg']:
                                            row_dict[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                            row_dict[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                                    row_dict[f'{prefix}_total_tradval'] = 0
                                    row_dict[f'{prefix}_total_money'] = 0
                        else:
                            for r in [row_total, row_otm, row_itm]:
                                for metric in ['delta', 'vega']:
                                    for sign in ['pos', 'neg']:
                                        r[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                        r[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                                r[f'{prefix}_total_tradval'] = 0
                                r[f'{prefix}_total_money'] = 0
                    else:
                        for r in [row_total, row_otm, row_itm]:
                            for metric in ['delta', 'vega']:
                                for sign in ['pos', 'neg']:
                                    r[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                    r[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                            r[f'{prefix}_total_tradval'] = 0
                            r[f'{prefix}_total_money'] = 0
                else:
                    for r in [row_total, row_otm, row_itm]:
                        for metric in ['delta', 'vega']:
                            for sign in ['pos', 'neg']:
                                r[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                r[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                        r[f'{prefix}_total_tradval'] = 0
                        r[f'{prefix}_total_money'] = 0
            
            if len(row_otm) > 2:
                total_data.append(row_total)
                otm_data.append(row_otm)
                itm_data.append(row_itm)
        except:
            pass
    
    return total_data, otm_data, itm_data

def create_precalculated_tables():
    """Create cache table"""
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
    print("✅ Cache table ready")

def precalculate_all_dates():
    """Pre-calculate with HYBRID RSI for all dates"""
    print("\n" + "="*80)
    print("PRE-CALCULATING DATA WITH HYBRID RSI")
    print("="*80 + "\n")
    
    dates = get_available_dates()
    if not dates:
        print("❌ No dates found")
        return
    
    print(f"📅 Found {len(dates)} dates\n")
    
    # Check existing cache
    try:
        existing_df = pd.read_sql(text("SELECT DISTINCT biz_date FROM options_dashboard_cache"), engine)
        existing_dates = set(pd.to_datetime(existing_df['biz_date']).dt.strftime('%Y-%m-%d'))
        print(f"📂 Cache has {len(existing_dates)} dates\n")
    except:
        existing_dates = set()
        print(f"📂 Starting fresh\n")
    
    processed, skipped = 0, 0
    
    for i, curr_date in enumerate(dates[:-1], 1):
        prev_date = get_prev_date(curr_date, dates)
        if not prev_date:
            continue
        
        if curr_date in existing_dates:
            print(f"[{i}/{len(dates)-1}] {curr_date} ⏭️")
            skipped += 1
            continue
        
        print(f"\n[{i}/{len(dates)-1}] {curr_date}:")
        
        try:
            total, otm, itm = calculate_and_store_data(curr_date, prev_date)
            
            if total and otm and itm:
                insert_sql = """
                INSERT INTO options_dashboard_cache (biz_date, prev_date, moneyness_type, data_json)
                VALUES (:curr, :prev, :type, :data)
                ON CONFLICT (biz_date, prev_date, moneyness_type)
                DO UPDATE SET data_json = EXCLUDED.data_json, created_at = CURRENT_TIMESTAMP
                """
                with engine.begin() as conn:
                    conn.execute(text(insert_sql), {"curr": curr_date, "prev": prev_date, "type": "TOTAL", "data": json.dumps(total)})
                    conn.execute(text(insert_sql), {"curr": curr_date, "prev": prev_date, "type": "OTM", "data": json.dumps(otm)})
                    conn.execute(text(insert_sql), {"curr": curr_date, "prev": prev_date, "type": "ITM", "data": json.dumps(itm)})
                processed += 1
                print(f"  ✅ Cached {len(total)} tickers")
            else:
                print(f"  ⚠️ No data")
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:50]}")
    
    print(f"\n" + "="*80)
    print(f"✅ COMPLETE: Processed={processed}, Skipped={skipped}")
    print("="*80)

if __name__ == '__main__':
    print("\n" + "="*80)
    print("DASHBOARD DATA PRE-CALCULATOR WITH HYBRID RSI")
    print("="*80)
    print("\nHYBRID RSI(40) SYSTEM:")
    print("  1. Try TradingView first (RSI 40-period)")
    print("  2. Fallback to Database (TA-Lib RSI 40-period)")
    print("  3. All other data from Database")
    print("\n" + "="*80)
    
    if TVDATAFEED_AVAILABLE:
        print("\n✅ TradingView enabled")
    else:
        print("\n⚠️ TradingView not available")
        print("Install: pip install tvDatafeed")
    
    if TALIB_AVAILABLE:
        print("✅ TA-Lib enabled (fallback)")
    else:
        print("⚠️ TA-Lib not available")
        print("Install TA-Lib for RSI fallback")
    
    print("\n" + "="*80)
    input("\nPress Enter to start...")
    
    create_precalculated_tables()
    precalculate_all_dates()
    
    print("\n✅ Done! Run dashboard_server.py")
    input("\nPress Enter to exit...")
