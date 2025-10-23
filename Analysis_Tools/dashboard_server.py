from flask import Flask, jsonify, request, render_template
from sqlalchemy import create_engine, text, inspect
import pandas as pd
from urllib.parse import quote_plus
import threading
import webbrowser
import time
import json

# Database config
db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'

db_password_enc = quote_plus(db_password)
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}')

app = Flask(__name__)

# CSV path for expiry dates
CSV_PATH = "C:\\Users\\Admin\\Desktop\\BhavCopy-Backup2\\complete.csv"

def get_available_dates():
    try:
        check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'options_dashboard_cache'
        );
        """
        exists = pd.read_sql(text(check_query), engine).iloc[0, 0]
        
        if not exists:
            print("⚠️  Cache table doesn't exist. Run update_database.py first!")
            return []
        
        query = "SELECT DISTINCT biz_date FROM options_dashboard_cache ORDER BY biz_date DESC"
        result = pd.read_sql(text(query), engine)
        
        if result.empty:
            print("⚠️  No dates in cache. Run update_database.py first!")
            return []
        
        return [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) for d in result['biz_date']]
    except Exception as e:
        print(f"❌ Error getting dates: {e}")
        return []

@app.route('/')
def index():
    dates = get_available_dates()
    if not dates:
        print("\n⚠️  No dates available!")
        print("   Please run: python update_database.py")
    else:
        print(f"✅ Found {len(dates)} dates available")
    return render_template('index.html', dates=dates)

@app.route('/stock/<ticker>')
def stock_detail(ticker):
    """Stock detail page"""
    dates = get_available_dates()
    return render_template('stock_detail.html', ticker=ticker, dates=dates)

@app.route('/get_available_tickers')
def get_available_tickers():
    """Get list of all available tickers"""
    try:
        inspector = inspect(engine)
        tables = [t for t in inspector.get_table_names() if t.startswith("TBL_") and t.endswith("_DERIVED")]
        tickers = sorted([t.replace("TBL_", "").replace("_DERIVED", "") for t in tables])
        return jsonify(tickers)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_available_trading_dates')
def get_available_trading_dates():
    """Get available trading dates from database"""
    try:
        dates = get_available_dates()
        return jsonify({'dates': dates})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_expiry_dates')
def get_expiry_dates_route():
    """Get expiry dates for a ticker"""
    ticker = request.args.get('ticker')
    if not ticker:
        return jsonify({'error': 'Ticker required'}), 400
    
    try:
        expiry_dates = get_expiry_dates_for_ticker(ticker)
        return jsonify({'expiry_dates': expiry_dates})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_expiry_data_detailed')
def get_expiry_data_detailed():
    """Get detailed data for each expiry date"""
    ticker = request.args.get('ticker')
    date = request.args.get('date')
    
    if not ticker or not date:
        return jsonify({'error': 'Ticker and date required'}), 400
    
    try:
        table_name = f"TBL_{ticker}_DERIVED"
        
        # Check if table exists
        inspector = inspect(engine)
        if table_name not in inspector.get_table_names():
            return jsonify({'error': f'Ticker {ticker} not found'}), 404
        
        # Get unique expiry dates with aggregated data
        query = f"""
        SELECT 
            "FininstrmActlXpryDt" as expiry,
            MAX("UndrlygPric") as price,
            SUM("TtlTradgVol") as volume,
            SUM("OpnIntrst") as oi,
            SUM("ChngInOpnIntrst") as oi_chg
        FROM "{table_name}"
        WHERE "BizDt" = :date
        AND "FininstrmActlXpryDt" IS NOT NULL
        GROUP BY "FininstrmActlXpryDt"
        ORDER BY "FininstrmActlXpryDt"
        """
        
        df = pd.read_sql(text(query), engine, params={"date": date})
        
        if df.empty:
            return jsonify({'expiry_data': [], 'lot_size': 0, 'fair_price': 0})
        
        # Convert to numeric
        for col in ['price', 'volume', 'oi', 'oi_chg']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Get lot size from complete.csv
        try:
            csv_df = pd.read_csv(CSV_PATH)
            ticker_csv = csv_df[csv_df['name'] == ticker]
            lot_size = int(ticker_csv['lot_size'].iloc[0]) if not ticker_csv.empty else 0
        except:
            lot_size = 0
        
        # Calculate fair price (ATM price from nearest expiry)
        fair_price = float(df['price'].iloc[0]) if not df.empty else 0
        
        # Build result
        result = []
        for _, row in df.iterrows():
            expiry_date = row['expiry'].strftime('%Y-%m-%d') if pd.notna(row['expiry']) else None
            result.append({
                'expiry': expiry_date,
                'price': float(row['price']) if pd.notna(row['price']) else 0,
                'volume': float(row['volume']) if pd.notna(row['volume']) else 0,
                'oi': float(row['oi']) if pd.notna(row['oi']) else 0,
                'oi_chg': float(row['oi_chg']) if pd.notna(row['oi_chg']) else 0
            })
        
        return jsonify({
            'expiry_data': result,
            'lot_size': lot_size,
            'fair_price': round(fair_price, 2)
        })
    
    except Exception as e:
        print(f"Error in get_expiry_data_detailed: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/get_stock_data')
def get_stock_data():
    """Get stock option chain data"""
    ticker = request.args.get('ticker')
    mode = request.args.get('mode', 'latest')
    expiry = request.args.get('expiry')
    date = request.args.get('date')
    
    try:
        # Get expiry dates from CSV
        expiry_dates = get_expiry_dates_for_ticker(ticker)
        
        # If no expiry specified, use the first one
        if not expiry and expiry_dates:
            expiry = expiry_dates[0]
        
        # Determine which date to use
        if mode == 'latest':
            available_dates = get_available_dates()
            if not available_dates:
                return jsonify({'error': 'No dates available'}), 404
            query_date = available_dates[0]
        else:
            query_date = date if date else get_available_dates()[0]
        
        # Get data from database
        table_name = f"TBL_{ticker}_DERIVED"
        
        # Check if table exists
        inspector = inspect(engine)
        if table_name not in inspector.get_table_names():
            return jsonify({'error': f'Ticker {ticker} not found'}), 404
        
        # Get option chain data
        query = f"""
        SELECT 
            "BizDt",
            "StrkPric",
            "OptnTp",
            "OpnIntrst",
            "ChngInOpnIntrst",
            "TtlTradgVol",
            "LastPric",
            "UndrlygPric",
            "ClsPric",
            "FininstrmActlXpryDt",
            "iv"
        FROM "{table_name}"
        WHERE "BizDt" = :date
        AND "OptnTp" IN ('CE', 'PE')
        AND "StrkPric" IS NOT NULL
        """
        
        if expiry and expiry != 'all':
            query += " AND \"FininstrmActlXpryDt\" = :expiry"
            df = pd.read_sql(text(query), engine, params={"date": query_date, "expiry": expiry})
        else:
            df = pd.read_sql(text(query), engine, params={"date": query_date})
            if not df.empty:
                df['FininstrmActlXpryDt'] = pd.to_datetime(df['FininstrmActlXpryDt'], errors='coerce')
                nearest_expiry = df['FininstrmActlXpryDt'].min()
                df = df[df['FininstrmActlXpryDt'] == nearest_expiry]
        
        if df.empty:
            return jsonify({'error': 'No data found for selected parameters'}), 404
        
        # Convert columns to numeric
        for col in ['StrkPric', 'OpnIntrst', 'ChngInOpnIntrst', 'TtlTradgVol', 'LastPric', 'UndrlygPric', 'ClsPric', 'iv']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Build option chain
        option_chain = build_option_chain(df)
        
        # Calculate stats
        stats = calculate_stats(df)
        
        # Get price data with all indicators for chart
        price_data = get_price_data_with_indicators(ticker, query_date, df)
        
        # Debug: Check what we got
        print(f"📊 Returning data for {ticker}: {len(option_chain)} strikes, {len(price_data)} price points")
        
        return jsonify({
            'ticker': ticker,
            'expiry_dates': expiry_dates,
            'selected_expiry': expiry,
            'last_updated': query_date,
            'stats': stats,
            'option_chain': option_chain,
            'price_data': price_data if price_data else []
        })
        
    except Exception as e:
        print(f"Error in get_stock_data: {e}")
        return jsonify({'error': str(e)}), 500

def get_expiry_dates_for_ticker(ticker):
    """Get expiry dates from CSV or database for a specific ticker"""
    try:
        # First try CSV
        df = pd.read_csv(CSV_PATH)
        ticker_df = df[df['name'] == ticker].copy()
        
        if not ticker_df.empty:
            ticker_df['expiry'] = pd.to_datetime(ticker_df['expiry'], errors='coerce')
            ticker_df = ticker_df.dropna(subset=['expiry'])
            expiry_dates = sorted(ticker_df['expiry'].dt.strftime('%Y-%m-%d').unique())
            if expiry_dates:
                return expiry_dates
        
        # If CSV doesn't have data, get from database
        table_name = f"TBL_{ticker}_DERIVED"
        inspector = inspect(engine)
        if table_name in inspector.get_table_names():
            query = f"""
            SELECT DISTINCT "FininstrmActlXpryDt"
            FROM "{table_name}"
            WHERE "FininstrmActlXpryDt" IS NOT NULL
            ORDER BY "FininstrmActlXpryDt"
            """
            result = pd.read_sql(text(query), engine)
            if not result.empty:
                result['FininstrmActlXpryDt'] = pd.to_datetime(result['FininstrmActlXpryDt'], errors='coerce')
                expiry_dates = [d.strftime('%Y-%m-%d') for d in result['FininstrmActlXpryDt'] if pd.notna(d)]
                return expiry_dates
        
        return []
    except Exception as e:
        print(f"Error getting expiry dates: {e}")
        return []

def build_option_chain(df):
    """Build option chain from dataframe"""
    strikes = sorted(df['StrkPric'].unique())
    option_chain = []
    
    for strike in strikes:
        ce_data = df[(df['StrkPric'] == strike) & (df['OptnTp'] == 'CE')]
        pe_data = df[(df['StrkPric'] == strike) & (df['OptnTp'] == 'PE')]
        
        row = {
            'strike': float(strike),
            'call_oi': float(ce_data['OpnIntrst'].iloc[0]) if not ce_data.empty else 0,
            'call_oi_chg': float(ce_data['ChngInOpnIntrst'].iloc[0]) if not ce_data.empty else 0,
            'call_volume': float(ce_data['TtlTradgVol'].iloc[0]) if not ce_data.empty else 0,
            'call_price': float(ce_data['LastPric'].iloc[0]) if not ce_data.empty else 0,
            'put_price': float(pe_data['LastPric'].iloc[0]) if not pe_data.empty else 0,
            'put_volume': float(pe_data['TtlTradgVol'].iloc[0]) if not pe_data.empty else 0,
            'put_oi_chg': float(pe_data['ChngInOpnIntrst'].iloc[0]) if not pe_data.empty else 0,
            'put_oi': float(pe_data['OpnIntrst'].iloc[0]) if not pe_data.empty else 0,
        }
        option_chain.append(row)
    
    return option_chain

def calculate_stats(df):
    """Calculate summary statistics"""
    ce_df = df[df['OptnTp'] == 'CE']
    pe_df = df[df['OptnTp'] == 'PE']
    
    total_ce_oi = float(ce_df['OpnIntrst'].sum())
    total_pe_oi = float(pe_df['OpnIntrst'].sum())
    total_ce_oi_chg = float(ce_df['ChngInOpnIntrst'].sum())
    total_pe_oi_chg = float(pe_df['ChngInOpnIntrst'].sum())
    pcr_oi = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0
    
    return {
        'total_ce_oi': total_ce_oi,
        'total_pe_oi': total_pe_oi,
        'total_ce_oi_chg': total_ce_oi_chg,
        'total_pe_oi_chg': total_pe_oi_chg,
        'pcr_oi': pcr_oi
    }

def get_yahoo_ticker_mapping():
    """Get all available tickers from database and create Yahoo Finance mapping"""
    try:
        inspector = inspect(engine)
        tables = [t for t in inspector.get_table_names() if t.startswith("TBL_") and t.endswith("_DERIVED")]
        tickers = [t.replace("TBL_", "").replace("_DERIVED", "") for t in tables]
        
        # Create mapping for all tickers
        mapping = {
            'NIFTY': '^NSEI',
            'BANKNIFTY': '^NSEBANK',
            'FINNIFTY': 'NIFTY_FIN_SERVICE.NS',
            'MIDCPNIFTY': '^NSEMDCP50',
        }
        
        # Add all other tickers with .NS suffix
        for ticker in tickers:
            if ticker not in mapping:
                mapping[ticker] = f"{ticker}.NS"
        
        print(f"✅ Created Yahoo Finance mapping for {len(mapping)} tickers")
        return mapping
    except Exception as e:
        print(f"Error creating ticker mapping: {e}")
        return {}

# Load ticker mapping once at startup
YAHOO_TICKER_MAP = get_yahoo_ticker_mapping()

def get_price_data_with_indicators(ticker, end_date, options_df):
    """Get intraday price data with OI, IV, PCR indicators"""
    try:
        import yfinance as yf
        from datetime import datetime, timedelta
        
        # Use the global mapping
        yahoo_symbol = YAHOO_TICKER_MAP.get(ticker, f"{ticker}.NS")
        
        print(f"📊 Fetching {ticker} as {yahoo_symbol}...")
        
        if isinstance(end_date, str):
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            end_dt = end_date
        
        start_dt = end_dt
        end_dt_plus = end_dt + timedelta(days=1)
        
        stock = yf.Ticker(yahoo_symbol)
        price_df = stock.history(start=start_dt, end=end_dt_plus, interval='5m')
        
        # If no data with .NS, try .BO (BSE)
        if price_df.empty and yahoo_symbol.endswith('.NS'):
            yahoo_symbol_bse = yahoo_symbol.replace('.NS', '.BO')
            print(f"⚠️ No data for {yahoo_symbol}, trying {yahoo_symbol_bse}...")
            stock = yf.Ticker(yahoo_symbol_bse)
            price_df = stock.history(start=start_dt, end=end_dt_plus, interval='5m')
        
        if price_df.empty:
            print(f"⚠️ No yfinance data for {ticker}, using fallback")
            return get_fallback_chart_data(ticker, end_date, options_df)
        
        print(f"✅ Got {len(price_df)} price data points")
        
        # Calculate indicators from options data
        ce_df = options_df[options_df['OptnTp'] == 'CE']
        pe_df = options_df[options_df['OptnTp'] == 'PE']
        
        total_ce_oi = float(ce_df['OpnIntrst'].sum()) if not ce_df.empty else 0
        total_pe_oi = float(pe_df['OpnIntrst'].sum()) if not pe_df.empty else 0
        total_ce_vol = float(ce_df['TtlTradgVol'].sum()) if not ce_df.empty else 0
        total_pe_vol = float(pe_df['TtlTradgVol'].sum()) if not pe_df.empty else 0
        
        pcr_oi = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 1.0
        pcr_vol = total_pe_vol / total_ce_vol if total_ce_vol > 0 else 1.0
        
        # Average IV from all options
        avg_iv = float(options_df['iv'].mean()) if 'iv' in options_df.columns else 0
        
        chart_data = []
        
        for timestamp, row in price_df.iterrows():
            unix_timestamp = int(timestamp.timestamp())
            
            chart_data.append({
                'time': unix_timestamp,
                'open': float(row['Open']),
                'high': float(row['High']),
                'low': float(row['Low']),
                'close': float(row['Close']),
                'volume': float(row['Volume']),
                'vwap': float((row['High'] + row['Low'] + row['Close']) / 3),
                'oi': total_ce_oi + total_pe_oi,  # Total OI
                'iv': avg_iv,  # Average IV
                'pcr': pcr_oi  # PCR based on OI
            })
        
        return chart_data
        
    except Exception as e:
        print(f"⚠️ Error getting chart data: {e}")
        return get_fallback_chart_data(ticker, end_date, options_df)

def get_fallback_chart_data(ticker, end_date, options_df):
    """Fallback chart data from database"""
    try:
        from datetime import datetime
        
        ce_df = options_df[options_df['OptnTp'] == 'CE']
        pe_df = options_df[options_df['OptnTp'] == 'PE']
        
        total_ce_oi = float(ce_df['OpnIntrst'].sum()) if not ce_df.empty else 0
        total_pe_oi = float(pe_df['OpnIntrst'].sum()) if not pe_df.empty else 0
        total_vol = float(options_df['TtlTradgVol'].sum())
        
        pcr_oi = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 1.0
        avg_iv = float(options_df['iv'].mean()) if 'iv' in options_df.columns else 0
        
        price = float(options_df['UndrlygPric'].iloc[0])
        
        if isinstance(end_date, str):
            date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            date_obj = end_date
        
        date_obj = date_obj.replace(hour=15, minute=30, second=0)
        unix_timestamp = int(date_obj.timestamp())
        
        return [{
            'time': unix_timestamp,
            'open': price,
            'high': price,
            'low': price,
            'close': price,
            'volume': total_vol,
            'vwap': price,
            'oi': total_ce_oi + total_pe_oi,
            'iv': avg_iv,
            'pcr': pcr_oi
        }]
    except Exception as e:
        print(f"Error in fallback chart data: {e}")
        return []
    """Get historical intraday price data using yfinance"""
    try:
        import yfinance as yf
        from datetime import datetime, timedelta
        
        # Map ticker to Yahoo Finance symbol
        yahoo_ticker_map = {
            'NIFTY': '^NSEI',
            'BANKNIFTY': '^NSEBANK',
            'FINNIFTY': 'NIFTY_FIN_SERVICE.NS'
        }
        
        yahoo_symbol = yahoo_ticker_map.get(ticker, f"{ticker}.NS")
        
        # Parse the end_date
        if isinstance(end_date, str):
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            end_dt = end_date
        
        # For dates in the past, get intraday data
        start_dt = end_dt
        end_dt_plus = end_dt + timedelta(days=1)
        
        print(f"Fetching yfinance data for {yahoo_symbol} on {end_date}...")
        
        # Download intraday data
        stock = yf.Ticker(yahoo_symbol)
        df = stock.history(start=start_dt, end=end_dt_plus, interval='5m')
        
        if df.empty:
            print(f"⚠️ No yfinance data for {yahoo_symbol} on {end_date}. Using fallback.")
            return get_price_data_from_db(ticker, end_date)
        
        print(f"✅ Got {len(df)} data points from yfinance")
        
        # Prepare data for chart
        price_data = []
        
        for timestamp, row in df.iterrows():
            # Convert timestamp to Unix timestamp (seconds since epoch)
            unix_timestamp = int(timestamp.timestamp())
            
            price_data.append({
                'time': unix_timestamp,
                'open': float(row['Open']),
                'high': float(row['High']),
                'low': float(row['Low']),
                'close': float(row['Close']),
                'volume': float(row['Volume'])
            })
        
        return price_data
        
    except ImportError:
        print("⚠️ yfinance not installed. Install with: pip install yfinance")
        print("   Falling back to database data...")
        return get_price_data_from_db(ticker, end_date)
    except Exception as e:
        print(f"⚠️ Error getting yfinance data: {e}")
        print("   Falling back to database data...")
        return get_price_data_from_db(ticker, end_date)

def get_price_data_from_db(ticker, end_date):
    """Fallback: Get price data from database"""
    try:
        from datetime import datetime
        
        table_name = f"TBL_{ticker}_DERIVED"
        
        query = f"""
        SELECT 
            "BizDt",
            "UndrlygPric"
        FROM "{table_name}"
        WHERE "BizDt" = :end_date
        LIMIT 1
        """
        
        df = pd.read_sql(text(query), engine, params={"end_date": end_date})
        
        if df.empty:
            return []
        
        df['UndrlygPric'] = pd.to_numeric(df['UndrlygPric'], errors='coerce')
        
        price = float(df['UndrlygPric'].iloc[0])
        
        # Parse date and create timestamp for 3:30 PM
        if isinstance(end_date, str):
            date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            date_obj = end_date
        
        # Set time to 15:30 (market close)
        date_obj = date_obj.replace(hour=15, minute=30, second=0)
        unix_timestamp = int(date_obj.timestamp())
        
        # Return single data point as OHLC
        return [{
            'time': unix_timestamp,
            'open': price,
            'high': price,
            'low': price,
            'close': price,
            'volume': 0
        }]
    except Exception as e:
        print(f"Error getting DB price data: {e}")
        return []

@app.route('/get_historical_data')
def get_historical_data():
    ticker = request.args.get('ticker')
    curr_date = request.args.get('date')
    option_type = request.args.get('type')
    metric = request.args.get('metric', 'money')
    strike = request.args.get('strike')
    
    try:
        query_dates = """
        SELECT DISTINCT biz_date 
        FROM options_dashboard_cache 
        WHERE biz_date <= :curr_date
        ORDER BY biz_date DESC
        LIMIT 40
        """
        dates_df = pd.read_sql(text(query_dates), engine, params={"curr_date": curr_date})
        
        if dates_df.empty:
            return jsonify({'error': 'No historical data found'}), 404
        
        dates = [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) for d in dates_df['biz_date']]
        dates.reverse()
        
        historical_data = []
        
        for date in dates:
            try:
                query = """
                SELECT data_json FROM options_dashboard_cache 
                WHERE biz_date = :date AND moneyness_type = 'TOTAL'
                """
                result = pd.read_sql(text(query), engine, params={"date": date})
                
                if not result.empty:
                    data = json.loads(result.iloc[0]['data_json'])
                    ticker_data = next((item for item in data if item['stock'] == ticker), None)
                    
                    if ticker_data:
                        prefix = option_type
                        table_name = f"TBL_{ticker}_DERIVED"
                        
                        if metric == 'vega' and strike and strike != 'N/A':
                            query_sql = f"""
                            SELECT 
                                SUM(CASE WHEN "OptnTp" = 'PE' THEN "TtlTradgVol" ELSE 0 END) as put_volume,
                                SUM(CASE WHEN "OptnTp" = 'CE' THEN "TtlTradgVol" ELSE 0 END) as call_volume,
                                SUM(CASE WHEN "OptnTp" = 'PE' THEN "OpnIntrst" ELSE 0 END) as put_oi,
                                SUM(CASE WHEN "OptnTp" = 'CE' THEN "OpnIntrst" ELSE 0 END) as call_oi,
                                MAX(CASE WHEN "OptnTp" = :opt_type AND "StrkPric" = :strike THEN "vega" ELSE NULL END) as strike_vega,
                                MAX("UndrlygPric") as underlying_price
                            FROM "{table_name}"
                            WHERE "BizDt" = :date
                            """
                            opt_type_param = 'CE' if option_type == 'call' else 'PE'
                            query_result = pd.read_sql(text(query_sql), engine, params={"date": date, "opt_type": opt_type_param, "strike": float(strike)})
                        else:
                            query_sql = f"""
                            SELECT 
                                SUM(CASE WHEN "OptnTp" = 'PE' THEN "TtlTradgVol" ELSE 0 END) as put_volume,
                                SUM(CASE WHEN "OptnTp" = 'CE' THEN "TtlTradgVol" ELSE 0 END) as call_volume,
                                SUM(CASE WHEN "OptnTp" = 'PE' THEN "OpnIntrst" ELSE 0 END) as put_oi,
                                SUM(CASE WHEN "OptnTp" = 'CE' THEN "OpnIntrst" ELSE 0 END) as call_oi,
                                AVG(CASE WHEN "OptnTp" = :opt_type THEN "vega" ELSE NULL END) as avg_vega,
                                MAX("UndrlygPric") as underlying_price
                            FROM "{table_name}"
                            WHERE "BizDt" = :date
                            """
                            opt_type_param = 'CE' if option_type == 'call' else 'PE'
                            query_result = pd.read_sql(text(query_sql), engine, params={"date": date, "opt_type": opt_type_param})
                        
                        if not query_result.empty:
                            put_vol = float(query_result.iloc[0]['put_volume'] or 0)
                            call_vol = float(query_result.iloc[0]['call_volume'] or 0)
                            put_oi = float(query_result.iloc[0]['put_oi'] or 0)
                            call_oi = float(query_result.iloc[0]['call_oi'] or 0)
                            underlying_price = float(query_result.iloc[0]['underlying_price'] or 0)
                            
                            pcr_volume = put_vol / call_vol if call_vol > 0 else 0
                            pcr_oi = put_oi / call_oi if call_oi > 0 else 0
                            
                            data_point = {
                                'date': date,
                                'pcr_volume': round(pcr_volume, 4),
                                'pcr_oi': round(pcr_oi, 4),
                                'underlying_price': round(underlying_price, 2),
                                'rsi': ticker_data.get('rsi', None)
                            }
                            
                            if metric == 'money':
                                moneyness = ticker_data.get(f'{prefix}_total_money', 0)
                                data_point['moneyness'] = float(moneyness)
                            elif metric == 'vega':
                                if strike and strike != 'N/A':
                                    strike_vega = float(query_result.iloc[0].get('strike_vega') or 0)
                                    data_point['strike_vega'] = round(strike_vega, 6)
                                else:
                                    avg_vega = float(query_result.iloc[0]['avg_vega'] or 0)
                                    data_point['avg_vega'] = round(avg_vega, 6)
                            
                            historical_data.append(data_point)
            except:
                continue
        
        return jsonify({
            'ticker': ticker,
            'option_type': option_type,
            'metric': metric,
            'strike': strike,
            'data': historical_data
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_data')
def get_data():
    curr_date = request.args.get('date')
    
    try:
        query = """
        SELECT biz_date, prev_date, moneyness_type, data_json 
        FROM options_dashboard_cache 
        WHERE biz_date = :date
        """
        result = pd.read_sql(text(query), engine, params={"date": curr_date})
        
        if result.empty:
            return jsonify({'error': 'No data found'}), 404
        
        total_row = result[result['moneyness_type'] == 'TOTAL'].iloc[0] if not result[result['moneyness_type'] == 'TOTAL'].empty else None
        otm_row = result[result['moneyness_type'] == 'OTM'].iloc[0]
        itm_row = result[result['moneyness_type'] == 'ITM'].iloc[0]
        
        response_data = {
            'curr_date': curr_date,
            'prev_date': str(otm_row['prev_date']),
            'otm': json.loads(otm_row['data_json']),
            'itm': json.loads(itm_row['data_json'])
        }
        
        if total_row is not None:
            response_data['total'] = json.loads(total_row['data_json'])
        else:
            response_data['total'] = []
        
        return jsonify(response_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

import socket

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "Unable to get IP"

def open_browser():
    time.sleep(1.5)
    webbrowser.open('http://localhost:5000')

if __name__ == '__main__':
    print("="*80)
    print("📊 STARTING DASHBOARD SERVER")
    print("="*80)
    
    local_ip = get_local_ip()
    
    print("\n✅ Server URLs:")
    print(f"   Local:   http://localhost:5000")
    print(f"   Network: http://{local_ip}:5000")
    print("\n📱 Share the Network URL with others on your network")
    print("✅ Auto-opening browser...")
    
    dates = get_available_dates()
    if not dates:
        print("\n" + "="*80)
        print("⚠️  WARNING: NO DATES AVAILABLE")
        print("="*80)
        print("\nThe cache table is empty or doesn't exist.")
        print("Please run this command first:")
        print("\n  cd Database")
        print("  python update_database.py")
        print("\n" + "="*80)
    
    print("\nPress Ctrl+C to stop\n")
    
    threading.Thread(target=open_browser, daemon=True).start()
    
    app.run(debug=False, host='0.0.0.0', port=5000)
