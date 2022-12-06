# -*- coding: utf-8 -*-
import pandas as pd
import math
import time
import os
import warnings
import pymssql as pyd
import datetime
import shutil
import ftplib
import logging

#エラー無視
warnings.simplefilter('ignore')

'''DB接続情報'''
# 接続情報(10.100.1.112)
server1 = "10.100.1.112"
database1 = "master"
username1 = "sa"
password1 = "p@$$w0rd"
conn1 =pyd.connect(server=server1, user=username1, password=password1, database=database1)

# 接続情報(10.106.1.238)
server2 = "10.106.1.238"
database2 = "ROWDB"
username2 = "sa"
password2 = "p@$$w0rd"
conn2 =pyd.connect(server=server2, user=username2, password=password2, database=database2)

# 接続情報(10.106.1.239)
server3 = "10.106.1.239"
database3 = "ROWDB"
username3 = "sa"
password3 = "p@$$w0rd"
conn3 = pyd.connect(server=server3, user=username3, password=password3, database=database3)

# 接続情報(10.106.1.240)
server4 = "10.106.1.240"
database4 = "ROWDB"
username4 = "sa"
password4 = "p@$$w0rd"
conn4 = pyd.connect(server=server4, user=username4, password=password4, database=database4) 

#ファイルの存在確認-三益-
if os.path.exists('三益/mimasu.csv'):
    l_list_m =pd.read_csv('三益/mimasu.csv', encoding='SHIFT-JIS')
else:
    pass

#出荷Lot用のクエリ作成
for x in range(len(l_list_m['KEY2'])):
    try:
        os.mkdir('三益/'+l_list_m['KEY2'][x])
    except Exception:
        pass
    #LotNoでループ
    query0 = 'declare @製造番号 nvarchar(50) =' + "'" + str(l_list_m['KEY2'][x]) + "';"+ '''
    declare @DRR_ID bigint;
    declare @SPG_ID bigint;
    declare @巻替範囲ID bigint;
    SELECT TOP (1)
    @DRR_ID = ID,
    @SPG_ID = cast(半製品ID as bigint),
    @巻替範囲ID = 巻替範囲ID
    FROM 生産管理システム.dbo.指令票_ROW_巻換え DRR
    where DRR.製造番号 = @製造番号 AND
    DRR.VOID IS NULL AND
    (DRR.出荷検査判定 IS NULL OR DRR.出荷検査判定 = 'OK')
    order by ID desc;
    SELECT TOP (1)
    DB接続名,
    @巻替範囲ID as 巻替範囲ID
    from 生産管理システム.dbo.ROW_T半製品 SPG
    inner join 生産管理システム.dbo.指令票_ROW_製作 DRM on DRM.ID = SPG.製作指令票ID
    inner join 生産管理システム.dbo.ROW_M製作機 MFM on MFM.号機名 = DRM.設備
    where SPG.ID = @SPG_ID;
    '''
    df0 = pd.read_sql(
                query0
                ,conn1
                )
    query1 ='''
    declare @製作履歴ID bigint;
    declare @日時St datetime;
    declare @日時Ed datetime;
    select
    @製作履歴ID = [製作履歴ID],
    @日時St = [日時St],
    @日時Ed = [日時Ed]
    from ROWDB.dbo.巻替範囲
    where [ID] = ''' + "'"+str(df0['巻替範囲ID'][0]) + "';" + '''
    with BASE AS
    (
    select *
    from ROWDB.dbo.[測定値] 
    where [製作履歴ID] = @製作履歴ID and [測定日時] >= @日時St and [測定日時] <= @日時Ed)
    select [測定値].*,
    [CSV時刻] AS FZ測定時刻,
    [カメラ1個別合計] AS [カメラ1個別合計],
    [カメラ1個別01] AS [カメラ1個別01],
    [カメラ1個別02] AS [カメラ1個別02],
    [カメラ1個別03]   AS [カメラ1個別03],
    [カメラ1個別04]   AS [カメラ1個別04],
    [カメラ1個別05]   AS [カメラ1個別05],
    [カメラ1個別06]   AS [カメラ1個別06],
    [カメラ1個別07]   AS [カメラ1個別07],
    [カメラ1個別08]   AS [カメラ1個別08],
    [カメラ2個別合計] AS [カメラ2個別合計],
    [カメラ2個別01]   AS [カメラ2個別01],
    [カメラ2個別02]   AS [カメラ2個別02],
    [カメラ2個別03]   AS [カメラ2個別03],
    [カメラ2個別04]   AS [カメラ2個別04],
    [カメラ2個別05]   AS [カメラ2個別05],
    [カメラ2個別06]   AS [カメラ2個別06],
    [カメラ2個別07]   AS [カメラ2個別07],
    [カメラ2個別08]   AS [カメラ2個別08],
    [カメラ3個別合計] AS [カメラ3個別合計],
    [カメラ3個別01]   AS [カメラ3個別01],
    [カメラ3個別02]   AS [カメラ3個別02],
    [カメラ3個別03]   AS [カメラ3個別03],
    [カメラ3個別04]   AS [カメラ3個別04],
    [カメラ3個別05]   AS [カメラ3個別05],
    [カメラ3個別06]   AS [カメラ3個別06],
    [カメラ3個別07]   AS [カメラ3個別07],
    [カメラ3個別08]   AS [カメラ3個別08]
    from BASE as [測定値]
    left join  ROWDB.dbo.[測定値_10分割] C on C.[測定値_測定日時] = [測定値].測定日時 
    and C.[測定値_製作履歴ID] = [測定値].製作履歴ID  
    order by [測定日時],製作履歴ID
    '''
    #240とつなぐ場合
    if df0['DB接続名'].values =='ROWDB_R3_240':
        print('240')
        df2 = pd.read_sql(
                    query1
                    ,conn4
                    )
        df2 = df2.loc[:,['ﾒｯｷ後カメラ1ダイヤ個数','ﾒｯｷ後カメラ2ダイヤ個数','ﾒｯｷ後カメラ3ダイヤ個数','ﾒｯｷ後カメラ1_3平均個数']]
        df2.to_csv('三益/'+str(l_list_m['KEY2'][x]+'/'+l_list_m['KEY2'][x])+'_個数.csv',encoding='SHIFT-JIS',index=False,header=True)
    #239とつなぐ場合
    elif df0['DB接続名'].values =='ROWDB_R3_239':
        print('239')
        df2 = pd.read_sql(
                    query1
                    ,conn3
                    )
        df2 = df2.loc[:,['ﾒｯｷ後カメラ1ダイヤ個数','ﾒｯｷ後カメラ2ダイヤ個数','ﾒｯｷ後カメラ3ダイヤ個数','ﾒｯｷ後カメラ1_3平均個数']]
        df2.to_csv('三益/'+str(l_list_m['KEY2'][x]+'/'+l_list_m['KEY2'][x])+'_個数.csv',encoding='SHIFT-JIS',index=False,header=True)
    else :
        print('no_data')
    #＿＿＿線径データ______
    #線径履歴から開始日時と終了日時を取得
    query2 = " SELECT * FROM ROWDB.dbo.巻替え_線径履歴 WHERE 製造番号 ="  + "'" +str(l_list_m['KEY2'][x]) + "'"
    df2 = pd.read_sql(
                    query2
                    ,conn2
                    )
    #クエリに流し込む変数を取得
    df2_id =df2.iat[0,0]
    df2_date_s =df2.at[0,'開始日時']
    df2_date_e =df2.at[0,'終了日時']
    #線径測定値から線径データをループして収集するための回数
    query3 =' SELECT count(履歴ID) FROM ROWDB.dbo.巻替え_線径測定値 WHERE 履歴ID= ' +str(df2_id) + " " +  "AND 日時  BETWEEN " + "'"+ str(df2_date_s) + "'" + " " +'AND' + " "  + "'" +str(df2_date_e) + "'"+ " " + "GROUP BY 履歴ID" 
    #forの繰り返し数を変数/10000で計算する
    ct = pd.read_sql(
                    query3
                    ,conn2
                    )
    #丸めた数値をループのカウント数に設定
    counts = math.ceil(ct.values/10000) +1
    #流し込むからのリスト作成
    data =[]
    #forループでぶっこぬき ※ SELECT文を文字列連結で作成
    for i in range(counts):
        if i == 0:
            query4 = ' SELECT * FROM ROWDB.dbo.巻替え_線径測定値 WHERE 履歴ID = ' + str(df2_id) + " " + 'AND' + " " + '日時  BETWEEN ' + "'"+ str(df2_date_s) + "'" + " " +'AND' + " "  + "'" +str(df2_date_e)  + "'" + " " + 'ORDER BY ID OFFSET ' + str(0) +' ' + 'ROWS FETCH NEXT ' + str(10000) + ' ' + 'ROWS ONLY'
            qv = pd.read_sql(
                query4,
                conn2
                )
            data = qv
            time.sleep(0.1)
        else:
            query5 = ' SELECT * FROM ROWDB.dbo.巻替え_線径測定値 WHERE 履歴ID = ' + str(df2_id) + " " + 'AND' + " " + '日時  BETWEEN ' + "'"+ str(df2_date_s) + "'" + " " +'AND' + " "  + "'" +str(df2_date_e)  + "'" + " " + 'ORDER BY ID OFFSET ' + str(1 + i * 10000) +' ' + 'ROWS FETCH NEXT ' + str(10000) + ' ' +'ROWS ONLY'
            qv = pd.read_sql(
                query5,
                conn2
                )
            data = pd.concat([data,qv],ignore_index=True)
            time.sleep(0.1)
            
    data.to_csv('三益/'+l_list_m['KEY2'][x]+'/'+str(l_list_m['KEY2'][x])+ '.csv',encoding='SHIFT-JIS')
    #必要な列数だけにする
    needs = pd.read_csv('三益/'+l_list_m['KEY2'][x]+'/'+str(l_list_m['KEY2'][x])+ '.csv', encoding = "SHIFT-JIS")
    needs.drop(columns = ['Unnamed: 0'], inplace=True)
    needs.columns =['ID','履歴ID','日時','線径μm']
    needs.to_csv('三益/'+l_list_m['KEY2'][x]+'/'+str(l_list_m['KEY2'][x])+'_線径.csv', index=False, encoding = "SHIFT-JIS")
    os.remove('三益/'+l_list_m['KEY2'][x]+'/'+str(l_list_m['KEY2'][x]) + '.csv')
    #zip圧縮
    shutil.make_archive('三益/'+l_list_m['KEY2'][x]+'/'+str(l_list_m['KEY2'][x]), 'zip', root_dir='三益/'+l_list_m['KEY2'][x]+'/'+str(l_list_m['KEY2'][x]))
    
    #FTP接続
    def ftp_upload(hostname, username, password, port, upload_src_path, upload_dst_path, timeout):
        logger.info({
            'action': 'ftp_upload',
            'status': 'run'
        })
        # FTP接続/アップロード
        with ftplib.FTP() as ftp:
            try:    
                ftp.connect(host=hostname, port=port, timeout=timeout)
                # パッシブモード設定
                ftp.set_pasv("true")
                # FTPサーバログイン
                ftp.login(username, password)
                with open(upload_src_path, 'rb') as fp:
                    ftp.storbinary(upload_dst_path, fp)
            except ftplib.all_errors as e:
                logger.error({
                    'action': 'ftp_upload',
                    'message': 'FTP error = %s' % e
                })
        logger.info({
            'action': 'ftp_upload',
            'status': 'success'
        })
    # logの設定
    logger = logging.getLogger(__name__)
    formatter = '%(asctime)s:%(name)s:%(levelname)s:%(message)s'
    logging.basicConfig(
        filename='./ftp_logger.log',
        level=logging.DEBUG,
        format=formatter
    )
    logger.setLevel(logging.INFO)
    # 接続先サーバーのホスト名
    hostname = "10.100.12.105" 
    # アップロードするファイルパス
    upload_src_path = '三益/'+l_list_m['KEY2'][x]+'/'+str(l_list_m['KEY2'][x])+'.zip'
    # アップロード先のファイルパス（STORはファイルをアップロードするためのFTPコマンドなので必要です。）
    upload_dst_path = "STOR "+ str(l_list_m['KEY2'][x]) + '.zip'
    # サーバーのユーザー名
    username = "test01" 
    # サーバーのログインパスワード
    password = "test01" 
    # FTPサーバポート
    port = 21 
    timeout = 50
    logger.info("===START FTP===")
    ftp_upload(hostname, username, password, port, upload_src_path, upload_dst_path, timeout)
    logger.info("===FINISH FTP===")
        
print('finish!')