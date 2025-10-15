import pandas as pd 
import os,psycopg2 
from sqlalchemy import create_engine 
import numpy as np
  
  
conn_string = 'postgresql+psycopg2://postgres:admin@127.0.0.1/fno_bhav'
  
db = create_engine(conn_string) 
conn = db.connect() 
  

# list_files=os.listdir("d:\\bn_data\\")
# for x in list_files:
#     if(".csv" in x):
#         df=pd.read_csv("d:\\bn_data\\"+x)
#         print(df)


  
 
#         df.to_sql('fo_bhvdata', con=conn, if_exists='append', 
#                 index=False) 
#         # conn = psycopg2.connect(conn_string) 
#         # conn.autocommit = True



  
sql1 = '''select distinct(bizdt) from fo_bhvdata;'''

# Works with pandas<2.2.0
df = pd.read_sql(
    sql=sql1,
    con=db,
)
list_dt=df.bizdt.to_list()
# conn.close()
print(list_dt)

for i in range(1,len(list_dt)-1,1):
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
    query="SELECT * FROM fo_bhvdata WHERE bizdt = '"+str(list_dt[i])+"' and opnintrst>0 and chnginopnintrst!=0 and chg_price!=200 and chg_price>0"
    # print(query)
    # output = conn.execute(query)
    # print(output.fetchall())
    # sql1 = '''select * from fo_bhvdata where "BizDt"=''''+str(x)
    # print(sql1)

    # Works with pandas<2.2.0
    df2 = pd.read_sql(
        sql=query,
        con=db,
    )
    print(df2)
    df3=pd.merge(df1, df2, on=['FinInstrmNm','FininstrmActlXpryDt'],how="inner")

    df3['chg_in_iv%']=round(100*((df3['IV_x']-df3['IV_y'])/((df3['IV_y']+df3['IV_x'])/2)),2)
    df3['chg_in_vega%']=round(100*((df3['Vega_x']-df3['Vega_y'])/((df3['Vega_y']+df3['Vega_x'])/2)),2)
    df3['chg_in_delta%']=round(100*((df3['Delta_x']-df3['Delta_y'])/((df3['Delta_y']+df3['Delta_x'])/2)),2)
    df3['chg_in_Theta%']=round(100*((df3['Theta_x']-df3['Theta_y'])/((df3['Theta_y']+df3['Theta_x'])/2)),2)
    df4=df3[['bizdt_y','FininstrmActlXpryDt','TckrSymb_y','FinInstrmNm','FinInstrmTp_y','StrkPric_y','OptnTp_y','UndrlygPric_y','SttlmPric_y','strike_diff_y','chg_oi_y',	'chg_price_y',	'chg_in_iv%','chg_in_vega%',	'chg_in_delta%',	'chg_in_Theta%']]
    # df3=pd.concat(df1.join(df2,on='FinInstrmNm',how="inner")
    df4['ce_rank']=np.where(((df4['StrkPric_y']<df4['UndrlygPric_y'])&(df4['OptnTp_y']=="CE")&(df4['chg_oi_y']<0) & (df4['chg_in_iv%']<0) & (df4['chg_in_vega%']<0) & (df4['chg_in_iv%']!=200) &  (df4['chg_in_iv%']!=-200)),1,np.where(((df4['StrkPric_y']<df4['UndrlygPric_y']) & (df4['OptnTp_y']=="CE")&(df4['chg_oi_y']>0) & (df4['chg_in_iv%']>0) & (df4['chg_in_vega%']>0) & (df4['chg_in_iv%']!=200) &  (df4['chg_in_iv%']!=-200)),-1,0))
    df4['pe_rank']=np.where(((df4['StrkPric_y']>df4['UndrlygPric_y'])&(df4['OptnTp_y']=="PE")&(df4['chg_oi_y']<0) & (df4['chg_in_iv%']<0) & (df4['chg_in_vega%']<0) & (df4['chg_in_iv%']!=200) &  (df4['chg_in_iv%']!=-200)),-1,np.where(((df4['StrkPric_y']>df4['UndrlygPric_y']) & (df4['OptnTp_y']=="PE")&(df4['chg_oi_y']>0) & (df4['chg_in_iv%']>0) & (df4['chg_in_vega%']>0) & (df4['chg_in_iv%']!=200) &  (df4['chg_in_iv%']!=-200)),1,0))
    df4['total_rank']=df4['ce_rank']+df4['pe_rank']
    print(df4)
    df4.to_csv("e:\\combined_"+str(list_dt[i])+".csv")