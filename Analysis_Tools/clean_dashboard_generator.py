from sqlalchemy import create_engine, inspect, text
import pandas as pd
from urllib.parse import quote_plus
from datetime import datetime

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
        result = pd.read_sql(text(f'SELECT DISTINCT "BizDt" FROM "{sample}" WHERE "BizDt" IS NOT NULL ORDER BY "BizDt" DESC LIMIT 10'), engine)
        return [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) for d in result['BizDt']] if not result.empty else []
    except:
        return []

def get_prev_date(curr, dates):
    try:
        i = dates.index(curr)
        return dates[i+1] if i+1 < len(dates) else None
    except:
        return None

def format_number(val):
    if val == 0:
        return '0'
    try:
        if abs(val) >= 1e9:
            return f"{val/1e9:.2f}B"
        elif abs(val) >= 1e6:
            return f"{val/1e6:.2f}M"
        elif abs(val) >= 1e3:
            return f"{val/1e3:.2f}K"
        else:
            return f"{val:.2f}"
    except:
        return str(val)

def generate_html(curr_date, prev_date, total_data, otm_data, itm_data):
    total_json = pd.DataFrame(total_data).to_json(orient='records')
    otm_json = pd.DataFrame(otm_data).to_json(orient='records')
    itm_json = pd.DataFrame(itm_data).to_json(orient='records')
    
    return f'''<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Options Analysis - {curr_date}</title>
<style>
* {{margin:0;padding:0;box-sizing:border-box}}
body {{font-family:'Segoe UI',Tahoma,Geneva,Verdana,sans-serif;background:#f5f7fa;padding:20px}}
.container {{max-width:2000px;margin:0 auto;background:white;border-radius:12px;box-shadow:0 4px 20px rgba(0,0,0,0.1);overflow:hidden}}
.header {{background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);color:white;padding:30px;text-align:center}}
.header h1 {{font-size:32px;margin-bottom:10px}}
.header .date {{font-size:18px;opacity:0.95}}
.controls {{display:flex;justify-content:space-between;align-items:center;padding:20px;background:#f8f9fa;border-bottom:2px solid #e9ecef}}
.tabs {{display:flex;gap:10px}}
.tab {{padding:12px 30px;border:none;background:#e9ecef;color:#495057;border-radius:8px;cursor:pointer;font-size:16px;font-weight:600;transition:all 0.3s}}
.tab:hover {{background:#dee2e6}}
.tab.active {{background:#667eea;color:white;box-shadow:0 2px 8px rgba(102,126,234,0.4)}}
.search-box {{flex:1;max-width:400px;margin-left:20px}}
.search-box input {{width:100%;padding:12px 20px;border:2px solid #dee2e6;border-radius:8px;font-size:15px}}
.search-box input:focus {{outline:none;border-color:#667eea;box-shadow:0 0 0 3px rgba(102,126,234,0.1)}}
.export-btn {{padding:12px 25px;background:#27ae60;color:white;border:none;border-radius:8px;cursor:pointer;font-size:15px;font-weight:600;margin-left:15px;transition:all 0.3s}}
.export-btn:hover {{background:#229954;transform:translateY(-2px);box-shadow:0 4px 12px rgba(39,174,96,0.3)}}
.tab-content {{display:none;padding:25px;overflow-x:auto}}
.tab-content.active {{display:block}}
table {{width:100%;border-collapse:collapse;font-size:13px;min-width:1800px}}
thead {{background:#667eea;color:white;position:sticky;top:0;z-index:10}}
thead tr:first-child th {{padding:15px 10px;text-align:center;font-size:14px;font-weight:700;border-right:1px solid rgba(255,255,255,0.2)}}
thead tr:nth-child(2) th {{padding:12px 8px;text-align:center;font-size:12px;font-weight:600;border-right:1px solid rgba(255,255,255,0.15)}}
.call-header {{background:#27ae60!important}}
.put-header {{background:#e74c3c!important}}
tbody tr {{transition:all 0.2s}}
tbody tr:hover {{background:#f1f3f5;transform:scale(1.002)}}
tbody tr:nth-child(even) {{background:#f8f9fa}}
tbody tr:nth-child(even):hover {{background:#f1f3f5}}
td {{padding:12px 8px;border-bottom:1px solid #e9ecef;border-right:1px solid #f0f0f0;text-align:center;white-space:nowrap;font-size:13px}}
.stock-name {{font-weight:700;color:#2c3e50;text-align:left!important;padding-left:20px!important;font-size:14px;position:sticky;left:0;background:white;z-index:5;box-shadow:2px 0 5px rgba(0,0,0,0.05)}}
tbody tr:nth-child(even) .stock-name {{background:#f8f9fa}}
tbody tr:hover .stock-name {{background:#f1f3f5}}
.strike-value {{font-weight:600;color:#495057;font-size:13px}}
.percentage {{font-size:12px;margin-left:4px;font-weight:600}}
.total-value {{font-weight:700;font-size:13px}}
.positive {{color:#27ae60}}
.negative {{color:#e74c3c}}
.no-results {{text-align:center;padding:60px;color:#999;font-size:18px}}
.stats {{display:flex;justify-content:center;gap:50px;padding:20px;background:#f8f9fa;border-bottom:1px solid #e9ecef}}
.stat-item {{text-align:center}}
.stat-value {{font-size:28px;font-weight:700;color:#667eea}}
.stat-label {{font-size:13px;color:#6c757d;margin-top:5px}}
.footer {{text-align:center;padding:20px;color:#6c757d;font-size:14px;border-top:1px solid #e9ecef;background:#f8f9fa}}
</style>
</head>
<body>
<div class="container">
<div class="header">
<h1>📊 Options Analysis Dashboard</h1>
<div class="date">📅 {prev_date} → {curr_date}</div>
</div>

<div class="stats">
<div class="stat-item">
<div class="stat-value">{len(total_data)}</div>
<div class="stat-label">Total Tickers</div>
</div>
<div class="stat-item">
<div class="stat-value">{len(otm_data)}</div>
<div class="stat-label">OTM Available</div>
</div>
<div class="stat-item">
<div class="stat-value">{len(itm_data)}</div>
<div class="stat-label">ITM Available</div>
</div>
</div>

<div class="controls">
<div class="tabs">
<button class="tab active" onclick="showTab('total')">📊 TOTAL</button>
<button class="tab" onclick="showTab('otm')">📈 OTM (Out of The Money)</button>
<button class="tab" onclick="showTab('itm')">📉 ITM (In The Money)</button>
</div>
<div class="search-box">
<input type="text" id="searchInput" placeholder="🔍 Search ticker... (e.g., NIFTY, RELIANCE, TCS)">
</div>
<button class="export-btn" onclick="exportToExcel()">📥 Export to Excel</button>
</div>

<div id="total-tab" class="tab-content active"></div>
<div id="otm-tab" class="tab-content"></div>
<div id="itm-tab" class="tab-content"></div>

<div class="footer">
Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Options Analysis Dashboard v3.2
</div>
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>
<script>
const totalData = {total_json};
const otmData = {otm_json};
const itmData = {itm_json};

function formatNumber(val) {{
    if (val === 0) return '0';
    const num = parseFloat(val);
    if (isNaN(num)) return val;
    if (Math.abs(num) >= 1e9) return (num/1e9).toFixed(2) + 'B';
    if (Math.abs(num) >= 1e6) return (num/1e6).toFixed(2) + 'M';
    if (Math.abs(num) >= 1e3) return (num/1e3).toFixed(2) + 'K';
    return num.toFixed(2);
}}

function generateTable(data) {{
if (!data || data.length === 0) {{
return '<div class="no-results">No data available</div>';
}}

let html = '<table><thead>';
html += '<tr>';
html += '<th rowspan="2" class="stock-name">Stock Name</th>';
html += '<th colspan="6" class="call-header">CALL OPTIONS</th>';
html += '<th colspan="6" class="put-header">PUT OPTIONS</th>';
html += '</tr>';
html += '<tr>';
html += '<th class="call-header">Δ+ Strike</th>';
html += '<th class="call-header">Δ- Strike</th>';
html += '<th class="call-header">Vega+ Strike</th>';
html += '<th class="call-header">Vega- Strike</th>';
html += '<th class="call-header">Total ΔTradingVal</th>';
html += '<th class="call-header">Total ΔMoneyness</th>';
html += '<th class="put-header">Δ+ Strike</th>';
html += '<th class="put-header">Δ- Strike</th>';
html += '<th class="put-header">Vega+ Strike</th>';
html += '<th class="put-header">Vega- Strike</th>';
html += '<th class="put-header">Total ΔTradingVal</th>';
html += '<th class="put-header">Total ΔMoneyness</th>';
html += '</tr></thead><tbody>';

data.forEach(row => {{
html += '<tr>';
html += `<td class="stock-name">${{row.stock}}</td>`;
html += `<td><span class="strike-value">${{row.call_delta_pos_strike}}</span><span class="percentage ${{parseFloat(row.call_delta_pos_pct) >= 0 ? 'positive' : 'negative'}}">${{row.call_delta_pos_pct}}%</span></td>`;
html += `<td><span class="strike-value">${{row.call_delta_neg_strike}}</span><span class="percentage ${{parseFloat(row.call_delta_neg_pct) >= 0 ? 'positive' : 'negative'}}">${{row.call_delta_neg_pct}}%</span></td>`;
html += `<td><span class="strike-value">${{row.call_vega_pos_strike}}</span><span class="percentage ${{parseFloat(row.call_vega_pos_pct) >= 0 ? 'positive' : 'negative'}}">${{row.call_vega_pos_pct}}%</span></td>`;
html += `<td><span class="strike-value">${{row.call_vega_neg_strike}}</span><span class="percentage ${{parseFloat(row.call_vega_neg_pct) >= 0 ? 'positive' : 'negative'}}">${{row.call_vega_neg_pct}}%</span></td>`;
html += `<td class="total-value ${{parseFloat(row.call_total_tradval) >= 0 ? 'positive' : 'negative'}}">${{formatNumber(row.call_total_tradval)}}</td>`;
html += `<td class="total-value ${{parseFloat(row.call_total_money) >= 0 ? 'positive' : 'negative'}}">${{formatNumber(row.call_total_money)}}</td>`;
html += `<td><span class="strike-value">${{row.put_delta_pos_strike}}</span><span class="percentage ${{parseFloat(row.put_delta_pos_pct) >= 0 ? 'positive' : 'negative'}}">${{row.put_delta_pos_pct}}%</span></td>`;
html += `<td><span class="strike-value">${{row.put_delta_neg_strike}}</span><span class="percentage ${{parseFloat(row.put_delta_neg_pct) >= 0 ? 'positive' : 'negative'}}">${{row.put_delta_neg_pct}}%</span></td>`;
html += `<td><span class="strike-value">${{row.put_vega_pos_strike}}</span><span class="percentage ${{parseFloat(row.put_vega_pos_pct) >= 0 ? 'positive' : 'negative'}}">${{row.put_vega_pos_pct}}%</span></td>`;
html += `<td><span class="strike-value">${{row.put_vega_neg_strike}}</span><span class="percentage ${{parseFloat(row.put_vega_neg_pct) >= 0 ? 'positive' : 'negative'}}">${{row.put_vega_neg_pct}}%</span></td>`;
html += `<td class="total-value ${{parseFloat(row.put_total_tradval) >= 0 ? 'positive' : 'negative'}}">${{formatNumber(row.put_total_tradval)}}</td>`;
html += `<td class="total-value ${{parseFloat(row.put_total_money) >= 0 ? 'positive' : 'negative'}}">${{formatNumber(row.put_total_money)}}</td>`;
html += '</tr>';
}});

html += '</tbody></table>';
return html;
}}

document.getElementById('total-tab').innerHTML = generateTable(totalData);
document.getElementById('otm-tab').innerHTML = generateTable(otmData);
document.getElementById('itm-tab').innerHTML = generateTable(itmData);

function showTab(tab) {{
document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
event.target.classList.add('active');
document.getElementById(tab + '-tab').classList.add('active');
}}

document.getElementById('searchInput').addEventListener('input', function(e) {{
const search = e.target.value.toLowerCase();
document.querySelectorAll('tbody tr').forEach(row => {{
const ticker = row.cells[0].textContent.toLowerCase();
row.style.display = ticker.includes(search) ? '' : 'none';
}});
}});

function exportToExcel() {{
const activeTab = document.querySelector('.tab-content.active');
const table = activeTab.querySelector('table');
if (!table) return;
const wb = XLSX.utils.table_to_book(table);
let tabName = 'TOTAL';
if (activeTab.id === 'otm-tab') tabName = 'OTM';
else if (activeTab.id === 'itm-tab') tabName = 'ITM';
XLSX.writeFile(wb, `Options_Analysis_${{tabName}}_{curr_date}.xlsx`);
}}
</script>
</body>
</html>'''

print("="*80)
print("📊 CLEAN DASHBOARD GENERATOR")
print("="*80)

dates = get_available_dates()
if not dates:
    print("❌ No dates!")
    input("\nPress Enter to exit...")
    exit()

print(f"\n✅ Found {len(dates)} dates")
for i, d in enumerate(dates[:5], 1):
    print(f"   {i}. {d}")

curr_date = input("\n📅 Enter date (YYYY-MM-DD): ").strip()
prev_date = get_prev_date(curr_date, dates)

if not prev_date:
    prev_date = input("Enter previous date: ").strip()
else:
    print(f"✅ Previous: {prev_date}")

print(f"\n📊 Processing: {prev_date} → {curr_date}\n")

inspector = inspect(engine)
tables = [t for t in inspector.get_table_names() if t.endswith("_DERIVED")]

otm_data = []
itm_data = []

for table in tables:
    try:
        ticker = table.replace("TBL_", "").replace("_DERIVED", "")
        print(f"{ticker:15s}...", end=" ")
        
        query = f'SELECT "BizDt","TckrSymb","StrkPric","OptnTp","UndrlygPric","delta","vega","strike_diff","TtlTrfVal" FROM "{table}" WHERE "BizDt" IN (:c,:p) AND "OptnTp" IN (\'CE\',\'PE\') AND "StrkPric" IS NOT NULL'
        df = pd.read_sql(text(query), engine, params={"c": curr_date, "p": prev_date})
        
        if df.empty:
            print("⚠️")
            continue
        
        for col in ['StrkPric','UndrlygPric','delta','vega','strike_diff','TtlTrfVal']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df['BizDt_str'] = df['BizDt'].astype(str)
        dfc = df[df['BizDt_str'] == curr_date].copy()
        dfp = df[df['BizDt_str'] == prev_date].copy()
        
        if dfc.empty or dfp.empty:
            print("⚠️")
            continue
        
        row_otm = {'stock': ticker}
        row_itm = {'stock': ticker}
        
        for opt_type, prefix in [('CE', 'call'), ('PE', 'put')]:
            dc = dfc[dfc['OptnTp']==opt_type].copy()
            dp = dfp[dfp['OptnTp']==opt_type].copy()
            
            if not dc.empty and not dp.empty:
                dm = pd.merge(dc, dp, on=['TckrSymb','StrkPric'], suffixes=('_c','_p'), how='inner')
                
                if not dm.empty:
                    dm['delta_chg'] = dm['delta_c'] - dm['delta_p']
                    dm['vega_chg'] = dm['vega_c'] - dm['vega_p']
                    dm['tradval_chg'] = dm['TtlTrfVal_c'] - dm['TtlTrfVal_p']
                    dm['money_chg'] = dm['strike_diff_c'] - dm['strike_diff_p']
                    dv = dm[(dm['delta_c'].notna())&(dm['delta_p'].notna())&(dm['delta_c']!=0)&(dm['delta_p']!=0)].copy()
                    
                    if not dv.empty:
                        # OTM
                        otm_cond = dv['strike_diff_c']>0 if opt_type=='CE' else dv['strike_diff_c']<0
                        dotm = dv[otm_cond]
                        
                        if not dotm.empty:
                            # Delta
                            idx_max = dotm['delta_chg'].idxmax()
                            idx_min = dotm['delta_chg'].idxmin()
                            row_otm[f'{prefix}_delta_pos_strike'] = f"{dotm.loc[idx_max,'StrkPric']:.0f}"
                            row_otm[f'{prefix}_delta_pos_pct'] = f"{dotm.loc[idx_max,'delta_chg']*100:.2f}"
                            row_otm[f'{prefix}_delta_neg_strike'] = f"{dotm.loc[idx_min,'StrkPric']:.0f}"
                            row_otm[f'{prefix}_delta_neg_pct'] = f"{dotm.loc[idx_min,'delta_chg']*100:.2f}"
                            # Vega
                            idx_max = dotm['vega_chg'].idxmax()
                            idx_min = dotm['vega_chg'].idxmin()
                            row_otm[f'{prefix}_vega_pos_strike'] = f"{dotm.loc[idx_max,'StrkPric']:.0f}"
                            row_otm[f'{prefix}_vega_pos_pct'] = f"{dotm.loc[idx_max,'vega_chg']*100:.2f}"
                            row_otm[f'{prefix}_vega_neg_strike'] = f"{dotm.loc[idx_min,'StrkPric']:.0f}"
                            row_otm[f'{prefix}_vega_neg_pct'] = f"{dotm.loc[idx_min,'vega_chg']*100:.2f}"
                            # Totals
                            row_otm[f'{prefix}_total_tradval'] = dotm['tradval_chg'].sum()
                            row_otm[f'{prefix}_total_money'] = dotm['money_chg'].sum()
                        else:
                            for metric in ['delta', 'vega']:
                                for sign in ['pos', 'neg']:
                                    row_otm[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                    row_otm[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                            row_otm[f'{prefix}_total_tradval'] = 0
                            row_otm[f'{prefix}_total_money'] = 0
                        
                        # ITM
                        itm_cond = dv['strike_diff_c']<0 if opt_type=='CE' else dv['strike_diff_c']>0
                        ditm = dv[itm_cond]
                        
                        if not ditm.empty:
                            # Delta
                            idx_max = ditm['delta_chg'].idxmax()
                            idx_min = ditm['delta_chg'].idxmin()
                            row_itm[f'{prefix}_delta_pos_strike'] = f"{ditm.loc[idx_max,'StrkPric']:.0f}"
                            row_itm[f'{prefix}_delta_pos_pct'] = f"{ditm.loc[idx_max,'delta_chg']*100:.2f}"
                            row_itm[f'{prefix}_delta_neg_strike'] = f"{ditm.loc[idx_min,'StrkPric']:.0f}"
                            row_itm[f'{prefix}_delta_neg_pct'] = f"{ditm.loc[idx_min,'delta_chg']*100:.2f}"
                            # Vega
                            idx_max = ditm['vega_chg'].idxmax()
                            idx_min = ditm['vega_chg'].idxmin()
                            row_itm[f'{prefix}_vega_pos_strike'] = f"{ditm.loc[idx_max,'StrkPric']:.0f}"
                            row_itm[f'{prefix}_vega_pos_pct'] = f"{ditm.loc[idx_max,'vega_chg']*100:.2f}"
                            row_itm[f'{prefix}_vega_neg_strike'] = f"{ditm.loc[idx_min,'StrkPric']:.0f}"
                            row_itm[f'{prefix}_vega_neg_pct'] = f"{ditm.loc[idx_min,'vega_chg']*100:.2f}"
                            # Totals
                            row_itm[f'{prefix}_total_tradval'] = ditm['tradval_chg'].sum()
                            row_itm[f'{prefix}_total_money'] = ditm['money_chg'].sum()
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
                                row_otm[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                row_otm[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                                row_itm[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                                row_itm[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                        row_otm[f'{prefix}_total_tradval'] = 0
                        row_otm[f'{prefix}_total_money'] = 0
                        row_itm[f'{prefix}_total_tradval'] = 0
                        row_itm[f'{prefix}_total_money'] = 0
                else:
                    for metric in ['delta', 'vega']:
                        for sign in ['pos', 'neg']:
                            row_otm[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                            row_otm[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                            row_itm[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                            row_itm[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                    row_otm[f'{prefix}_total_tradval'] = 0
                    row_otm[f'{prefix}_total_money'] = 0
                    row_itm[f'{prefix}_total_tradval'] = 0
                    row_itm[f'{prefix}_total_money'] = 0
            else:
                for metric in ['delta', 'vega']:
                    for sign in ['pos', 'neg']:
                        row_otm[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                        row_otm[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                        row_itm[f'{prefix}_{metric}_{sign}_strike'] = 'N/A'
                        row_itm[f'{prefix}_{metric}_{sign}_pct'] = '0.00'
                row_otm[f'{prefix}_total_tradval'] = 0
                row_otm[f'{prefix}_total_money'] = 0
                row_itm[f'{prefix}_total_tradval'] = 0
                row_itm[f'{prefix}_total_money'] = 0
        
        if len(row_otm) > 1:
            otm_data.append(row_otm)
            itm_data.append(row_itm)
            print("✅")
    except Exception as e:
        print(f"❌")

if otm_data and itm_data:
    html_file = f"C:\\Users\\Admin\\Desktop\\BhavCopy Backup2\\Reports\\Options_Clean_Dashboard_{curr_date}.html"
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(generate_html(curr_date, prev_date, otm_data, itm_data))
    
    print(f"\n✅ Clean Dashboard Created!")
    print(f"   File: {html_file}")
    print(f"   Tickers: {len(otm_data)}")
    print("\n📊 Open the HTML file in your browser!")
    input("\nPress Enter to exit...")
else:
    print("\n⚠️ No data!")
    input("\nPress Enter to exit...")
