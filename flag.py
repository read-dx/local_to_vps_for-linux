# -*- coding: utf-8 -*-
import pymssql as pyd
import pandas as pd
import warnings
import os
import time
import shutil
import subprocess as sp

warnings.simplefilter('ignore')

#10.100.1.112
server1 = "10.100.1.112"
database1 = "master"
username1 = "sa"
password1 = "p@$$w0rd"
conn1 =pyd.connect(server=server1, user=username1, password=password1, database=database1)
    

# 10.106.1.238
server2 = "10.106.1.238"
database2 = "ROWDB"
username2 = "sa"
password2 = "p@$$w0rd"
conn2 =pyd.connect(server=server2, user=username2, password=password2, database=database2)

# 10.106.1.239)
server3 = "10.106.1.239"
database3 = "ROWDB"
username3 = "sa"
password3 = "p@$$w0rd"
conn3 = pyd.connect(server=server3, user=username3, password=password3, database=database3)

# (10.106.1.240)
server4 = "10.106.1.240"
database4 = "ROWDB"
username4 = "sa"
password4 = "p@$$w0rd"
conn4 = pyd.connect(server=server4, user=username4, password=password4, database=database4) 

#check flag file
df = pd.read_csv('update_id.csv', encoding='SHIFT-JIS')
dfid =df.iloc[0,0] #get ID

c_name_m = '%三益%'
s_list_m = 'SELECT DISTINCT sou.KEY1,sou.KEY2, maki.納品距離/1000 as "納品距離" FROM 生産管理システム.dbo.T操作履歴 as sou INNER JOIN 生産管理システム.dbo.指令票_ROW_巻換え  as maki on sou.KEY1  = maki.ID WHERE 最終客先名 LIKE '+"'" + c_name_m +"'"+ ' AND sou.id >' +"'" + str(dfid) + "'"
s_list_m = pd.read_sql(
                s_list_m
                ,conn1
                )

if len(s_list_m['KEY1']) >= 1:
    s_list_m.to_csv('mimasu.csv',encoding='SHIFT-JIS',index=False) #製造Lotのリスト_三益
else: pass

#更新回数(SUMCO)
c_name_s = '%SUMCO%'
s_list_s = 'SELECT DISTINCT sou.KEY1,sou.KEY2,maki.納品距離/1000 as "納品距離"  FROM 生産管理システム.dbo.T操作履歴 as sou INNER JOIN 生産管理システム.dbo.指令票_ROW_巻換え  as maki on sou.KEY1  = maki.ID WHERE 最終客先名 LIKE '+"'" + c_name_s +"'"+ ' AND sou.id >' +"'" + str(dfid) + "'"
s_list_s = pd.read_sql(
                s_list_s
                ,conn1
                )
if len(s_list_s['KEY1']) >= 1:
    s_list_s.to_csv('sumco.csv',encoding='SHIFT-JIS',index=False) #製造Lotのリスト_SUMCO
else: pass

#更新回数(全体)
upquery = 'SELECT count(sou.ID) FROM 生産管理システム.dbo.T操作履歴 as sou INNER JOIN 生産管理システム.dbo.指令票_ROW_巻換え  as maki on sou.KEY1  = maki.ID WHERE sou.id >' +"'" + str(dfid) + "'"

uplist = pd.read_sql(
                upquery
                ,conn1
                )
data_count = uplist.iloc[0,0] 

#ファイルID更新
df = df + data_count
df.to_csv('update_id.csv', encoding='SHIFT-JIS', index=False)

#フォルダ可否確認
if os.path.exists('三益'):
    pass
else :
    os.mkdir('三益')

if os.path.exists('SUMCO'):
    pass
else :
    os.mkdir('SUMCO')

#ファイルを各フォルダに移動
if os.path.exists('mimasu.csv'):
    try:
        shutil.move('mimasu.csv', '三益/mimasu.csv')
        time.sleep(2)
        sp.run(['python3', '三益/getdata_m.py'])
    except Exception:
        pass
else :pass

if os.path.exists('sumco.csv'):
    try :
        shutil.move('sumco.csv', 'SUMCO/sumco.csv')
        time.sleep(2)
        sp.run(['python3', 'SUMCO/getdata_s.py'])
        time.sleep(2)
    except Exception:
        pass
    
else :pass

