
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import re
import pandas as pd 
import os
import numpy as np
  


# PostgreSQL Connection ===
db_user = 'postgres'
db_password = 'Gallop@3104'
db_host = 'localhost'
db_port = '5432'
db_name = 'BhavCopy_Database'

db_password_enc = quote_plus(db_password)

db = create_engine(
    f'postgresql+psycopg2://{db_user}:{db_password_enc}@{db_host}:{db_port}/{db_name}'
)
conn = db.connect() 

from py_vollib.black_scholes.implied_volatility import implied_volatility
from py_vollib.black_scholes.greeks.analytical import delta, gamma, rho, theta, vega
from datetime import datetime, timedelta



def greeks(premium, expiry,cd, asset_price, strike_price, intrest_rate, instrument_type):

    t = ((datetime(expiry.year, expiry.month, expiry.day, 15, 30) - (datetime(cd.year, cd.month, cd.day, 15, 30)))/timedelta(days=1))/365
    S = asset_price
    K = strike_price
    r = intrest_rate
    flag = instrument_type[0].lower()
    imp_v = implied_volatility(premium, S, K, t, r, flag)
    return {"IV": imp_v,
            "Delta": delta(flag, S, K, t, r, imp_v),
            "Gamma": gamma(flag, S, K, t, r, imp_v),
            "Rho": rho(flag, S, K, t, r, imp_v),
            "Theta": theta(flag, S, K, t, r, imp_v),
            "Vega": vega(flag, S, K, t, r, imp_v)}
list_files=os.listdir("C:\\Users\\Santoo\\Downloads\\bhav copy\\bhav copy\\")
for x in list_files:
    df=pd.read_csv("C:\\Users\\Santoo\\Downloads\\bhav copy\\bhav copy\\"+x)
    # print(df)
    df=df[['BizDt','Sgmt','FinInstrmTp','TckrSymb','FininstrmActlXpryDt','StrkPric','OptnTp','FinInstrmNm','OpnPric','HghPric','LwPric','ClsPric','LastPric','PrvsClsgPric','UndrlygPric','SttlmPric','OpnIntrst','ChngInOpnIntrst','TtlTradgVol','TtlTrfVal','TtlNbOfTxsExctd','NewBrdLotQty']]
    
    

    df.columns= df.columns.str.lower()
    df['strike_diff']=df['undrlygpric']-df['strkpric']
    df['y_oi']=df['opnintrst']-df['chnginopnintrst']
    df['chg_oi']=round(100*((df['y_oi']-df['opnintrst'])/((df['opnintrst']+df['y_oi'])/2)),2)
    df['chg_price']=round(100*((df['prvsclsgpric']-df['lastpric'])/((df['prvsclsgpric']+df['lastpric'])/2)),2)
    # print(df)
    # print(df['BizDt'][0])

    for index, row in df.iterrows():
        try:
            if("O" in row['fininstrmtp']):
                premium = row['lastpric']
                expiry = pd.to_datetime(row['fininstrmactlxprydt'])
                cd=pd.to_datetime(row['bizdt'])
                asset_price = row['undrlygpric']
                strike_price = row['strkpric']
                intrest_rate = 0.1
                if(row['optntp']=="PE"):
                    instrument_type = "p"
                if(row['optntp']=="CE"):
                    instrument_type = "c"
                value=greeks(premium,expiry,cd,asset_price,strike_price,intrest_rate,instrument_type)
                df.at[index, 'Delta'] =round(abs(float(value['Delta'])),4)
                df.at[index, 'Vega'] =round(abs(float(value['Vega'])),4)
                df.at[index, 'Theta'] =round(abs(float(value['Theta'])),4)
                df.at[index, 'IV'] =round(abs(float(value['IV'])),4)
        except:
            df.at[index, 'Delta'] =0
            df.at[index, 'Vega'] =0
            df.at[index, 'Theta'] =0
            df.at[index, 'IV'] =0
    df.to_csv("d:\\fo_derived_"+str(df['bizdt'][0])+".csv")
    df.to_sql('fo_bhvdata', con=conn, if_exists='append', index=False)    
    print(df)

##########   current
  
sql1 = '''select distinct(bizdt) from fo_bhvdata ;'''

# Works with pandas<2.2.0
df = pd.read_sql(
    sql=sql1,
    con=db,
)
list_dt_2=df.bizdt.to_list()
list_dt = list_dt_2[-4:]
# list_dt=df.bizdt.to_list()   ####### all
# conn.close()
print(list_dt)

# for i in range(1,len(list_dt),1):
#     query="SELECT * FROM fo_bhvdata WHERE bizdt = '"+str(list_dt[i-1])+"' and  opnintrst>0 and chnginopnintrst!=0 and chg_price!=200 and chg_price>0"
#     # print(query)
#     # output = conn.execute(query)
#     # print(output.fetchall())
#     # sql1 = '''select * from fo_bhvdata where "BizDt"=''''+str(x)
#     # print(sql1)

#     # Works with pandas<2.2.0
#     df1 = pd.read_sql(
#         sql=query,
#         con=db,
#     )
#     print(df1)
#     query="SELECT * FROM fo_bhvdata WHERE bizdt = '"+str(list_dt[i])+"' and opnintrst>0 and chnginopnintrst!=0 and chg_price!=200 and chg_price>0"

#     df2 = pd.read_sql(
#         sql=query,
#         con=db,
#     )
#     print(df2)
#     df3=pd.merge(df1, df2, on=['fininstrmnm','fininstrmactlxprydt'],how="inner")

#     df3['chg_in_iv%']=round(100*((df3['IV_x']-df3['IV_y'])/((df3['IV_y']+df3['IV_x'])/2)),2)
#     df3['chg_in_vega%']=round(100*((df3['Vega_x']-df3['Vega_y'])/((df3['Vega_y']+df3['Vega_x'])/2)),2)
#     df3['chg_in_delta%']=round(100*((df3['Delta_x']-df3['Delta_y'])/((df3['Delta_y']+df3['Delta_x'])/2)),2)
#     df3['chg_in_Theta%']=round(100*((df3['Theta_x']-df3['Theta_y'])/((df3['Theta_y']+df3['Theta_x'])/2)),2)
#     df4=df3[['bizdt_y','fininstrmactlxprydt','tckrsymb_y','fininstrmnm','fininstrmtp_y','strkpric_y','optntp_y','undrlygpric_y','sttlmpric_y','strike_diff_y','chg_oi_y',	'chg_price_y',	'chg_in_iv%','chg_in_vega%',	'chg_in_delta%',	'chg_in_Theta%']]
#     # df3=pd.concat(df1.join(df2,on='FinInstrmNm',how="inner")
#     df4['ce_rank']=np.where(((df4['strkpric_y']<df4['undrlygpric_y'])&(df4['optntp_y']=="CE")&(df4['chg_oi_y']<0) & (df4['chg_in_iv%']<0) & (df4['chg_in_vega%']<0) & (df4['chg_in_iv%']!=200) &  (df4['chg_in_iv%']!=-200)),1,np.where(((df4['strkpric_y']<df4['undrlygpric_y']) & (df4['optntp_y']=="CE")&(df4['chg_oi_y']>0) & (df4['chg_in_iv%']>0) & (df4['chg_in_vega%']>0) & (df4['chg_in_iv%']!=200) &  (df4['chg_in_iv%']!=-200)),-1,0))
#     df4['pe_rank']=np.where(((df4['strkpric_y']>df4['undrlygpric_y'])&(df4['optntp_y']=="PE")&(df4['chg_oi_y']<0) & (df4['chg_in_iv%']<0) & (df4['chg_in_vega%']<0) & (df4['chg_in_iv%']!=200) &  (df4['chg_in_iv%']!=-200)),-1,np.where(((df4['strkpric_y']>df4['undrlygpric_y']) & (df4['optntp_y']=="PE")&(df4['chg_oi_y']>0) & (df4['chg_in_iv%']>0) & (df4['chg_in_vega%']>0) & (df4['chg_in_iv%']!=200) &  (df4['chg_in_iv%']!=-200)),1,0))
#     df4['total_rank']=df4['ce_rank']+df4['pe_rank']
#     print(df4)
#     df4.to_csv("e:\\combined_"+str(list_dt[i])+".csv")
#     df4.to_sql('fo_bhvdata_processed_current', con=conn, if_exists='replace', index=False)    
#     print(df)


  
# sql1 = '''select distinct(bizdt) from fo_bhvdata;'''

# # Works with pandas<2.2.0
# df = pd.read_sql(
#     sql=sql1,
#     con=db,
# )
# list_dt=df.bizdt.to_list()
# # conn.close()
# print(list_dt)

for i in range(1,len(list_dt),1):
    query="SELECT * FROM fo_bhvdata WHERE bizdt = '"+str(list_dt[i-1])+"' and  opnintrst>0 and chnginopnintrst!=0 and chg_price!=200 and chg_price>0"
    # print(query)
    # output = conn.execute(query)
    # print(output.fetchall())
    # sql1 = '''select * from fo_bhvdata where "BizDt"=''''+str(x)
    # print(sql1)

    # Works with pandas<2.2.0
    df1 = pd.read_sql(
        sql=query,
        con=db,
    )
    print(df1)
    df1['cumulative_price_volume'] = (df1['sttlmpric'] * df1['ttltradgvol']).cumsum()
    df1['cumulative_volume'] = df1['ttltradgvol'].cumsum()
    df1['vwap'] = round(df1['cumulative_price_volume'] / df1['cumulative_volume'],2)
    df1['pd']=df1['prvsclsgpric']-df1['sttlmpric']
    df1['coi/volume'] = df1['chnginopnintrst']/df1['ttltradgvol']
    df1['money_flow']=df1['chnginopnintrst']*df1['vwap']


    query="SELECT * FROM fo_bhvdata WHERE bizdt = '"+str(list_dt[i])+"' and opnintrst>0 and chnginopnintrst!=0 and chg_price!=200 and chg_price>0"

    df2 = pd.read_sql(
        sql=query,
        con=db,
    )
    print(df2)
    df2['cumulative_price_volume'] = (df2['sttlmpric'] * df2['ttltradgvol']).cumsum()
    df2['cumulative_volume'] = df2['ttltradgvol'].cumsum()
    df2['vwap'] = round(df2['cumulative_price_volume'] / df2['cumulative_volume'],2)
    df2['pd']=df2['prvsclsgpric']-df2['sttlmpric']
    df2['coi/volume'] = df2['chnginopnintrst']/df2['ttltradgvol']
    df2['money_flow']=df2['chnginopnintrst']*df2['vwap']

    df3=pd.merge(df1, df2, on=['fininstrmnm','fininstrmactlxprydt'],how="inner")
    
    df3['money_flow_chg']=round(100*((df3['money_flow_x']-df3['money_flow_y'])/((df3['money_flow_y']+df3['money_flow_x'])/2)),2)
    df3['volume_chg'] = round(100*((df3['ttltradgvol_x']-df3['ttltradgvol_y'])/((df3['ttltradgvol_y']+df3['ttltradgvol_x'])/2)),2)
    df3['coi/volume_chg'] = df3['chnginopnintrst_y']/df3['volume_chg']

    df3['vwap_chg'] = round(100*((df3['vwap_x']-df3['vwap_y'])/((df3['vwap_y']+df3['vwap_x'])/2)),2)
    df3['chg_in_iv%']=round(100*((df3['IV_x']-df3['IV_y'])/((df3['IV_y']+df3['IV_x'])/2)),2)
    df3['chg_in_vega%']=round(100*((df3['Vega_x']-df3['Vega_y'])/((df3['Vega_y']+df3['Vega_x'])/2)),2)
    df3['chg_in_delta%']=round(100*((df3['Delta_x']-df3['Delta_y'])/((df3['Delta_y']+df3['Delta_x'])/2)),2)
    df3['chg_in_Theta%']=round(100*((df3['Theta_x']-df3['Theta_y'])/((df3['Theta_y']+df3['Theta_x'])/2)),2)
    df4=df3[['bizdt_y','fininstrmactlxprydt','tckrsymb_y','fininstrmnm','fininstrmtp_y','strkpric_y','optntp_y','undrlygpric_y','sttlmpric_y','strike_diff_y','chg_oi_y','money_flow_chg','volume_chg','coi/volume_chg','chg_price_y',	'chg_in_iv%','chg_in_vega%',	'chg_in_delta%',	'chg_in_Theta%']]
    # df3=pd.concat(df1.join(df2,on='FinInstrmNm',how="inner")
    df4['ce_rank']=np.where(((df4['optntp_y']=="CE")  & (df4['chg_in_vega%']<0) & (df4['chg_in_iv%']!=200) &  (df4['chg_in_iv%']!=-200)),1,np.where(( (df4['optntp_y']=="CE") & (df4['chg_in_vega%']>0) & (df4['chg_in_iv%']!=200) &  (df4['chg_in_iv%']!=-200)),-1,0))
    df4['pe_rank']=np.where(((df4['optntp_y']=="PE") & (df4['chg_in_vega%']<0) & (df4['chg_in_iv%']!=200) &  (df4['chg_in_iv%']!=-200)),-1,np.where(((df4['optntp_y']=="PE") & (df4['chg_in_vega%']>0) & (df4['chg_in_iv%']!=200) &  (df4['chg_in_iv%']!=-200)),1,0))
    df4['total_rank']=df4['ce_rank']+df4['pe_rank']
    print(df4)
    df4.to_csv("e:\\combined_"+str(list_dt[i])+".csv")
    df4.to_sql('fo_bhvdata_processed', con=conn, if_exists='append', index=False)    