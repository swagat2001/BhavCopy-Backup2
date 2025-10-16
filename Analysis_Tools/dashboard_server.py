from flask import Flask, jsonify, request, render_template
from sqlalchemy import create_engine, text
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

def get_available_dates():
    try:
        # Check if cache table exists
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
                        
                        # FIX: Get strike-specific vega instead of average
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
                                'rsi': ticker_data.get('rsi', None)  # Get RSI from cached data
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
    
    # For HTTPS, uncomment these lines and generate SSL certificate:
    # app.run(debug=False, host='0.0.0.0', port=5000, ssl_context='adhoc')
    
    # For HTTP (default):
    app.run(debug=False, host='0.0.0.0', port=5000)