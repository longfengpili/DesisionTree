#!/usr/bin/env python3
#-*- coding:utf-8 -*-

from DesisionTreeClasser import DecisionTreeClasser
from redshift import db_redshift
from setting import *
from datetime import datetime,date,timedelta
from pandas import DataFrame,Series
import pandas as pd

class TreeRun(DecisionTreeClasser,db_redshift):

    def __init__(self):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dir = model_dir
        self.random_state = random_state
        self.test_size = test_size
        self.columns = columns

    def update_retention_data(self,sql,date,app_name,puzzle_language,days):

        rows,result = self.redshift_select(sql,date=date,app_name=app_name,puzzle_language=puzzle_language,day=days)

        return rows,result
        
    def predict_user_retention_status(self,data):
        '''
        1、load model
        2、change data
        3、predict
        '''

        recent_model = self.load_recent_model()

        result_all = DataFrame(data,columns=self.columns)
        result = result_all.copy()
        result['first_nation'] = result['first_nation'].fillna('unknown')
        result['coin_after'] = result['coin_after'].fillna(200)
        result['enjoy_status'] = result['enjoy_status'].apply(lambda x : -1 if pd.isna(x) else 1 if x == 'enjoy-yes' else 0)
        result['iap_status'] = result['first_iap_date'].apply(lambda x:0 if pd.isna(x) else 1)
        result['retention_status'] = result['retention_ts'].apply(lambda x:0 if pd.isna(x) else 1)
        result = result.drop(columns=['first_iap_date','retention_ts','retention_predict'])

        x = result.iloc[:,6:13].values
        result_pre = recent_model.predict(x)
        result_pre = pd.concat([result_all.drop(columns=['retention_predict']),Series(result_pre)],axis=1)
        result_pre.columns = self.columns

        return result_pre

    def update_predict_info(self,data_pre):
        update_result = {}
        for i in data_pre.iterrows():
            sql = '''
                update report_word.user_info_before7_after7
                set retention_predict = '{}'
                where user_id = '{}' and app_name = '{}'
                and platform = '{}' and puzzle_language ='{}'
            '''.format(i[1].retention_predict,i[1].user_id,i[1].app_name,i[1].platform,i[1].puzzle_language)

            update_result.setdefault(i[1].install_date.strftime('%Y-%m-%d'),0)
            update_result[i[1].install_date.strftime('%Y-%m-%d')] += 1

            rows = self.redshift_insert(sql)
        
        return update_result
        


if __name__ == '__main__':
    # with open('./sqls/retention.sql','r',encoding='utf-8') as f:
    #     sql = f.read()

    sql = 'select * from report_word.user_info_before7_after7 limit 10'
    tree_run = TreeRun()
    date = date(2018,10,1)
    rows,result = tree_run.update_retention_data(sql,date,app_name,puzzle_language,days) #更新数据
    result_pre = tree_run.predict_user_retention_status(result)
    print(result_pre)
    update_result = tree_run.update_predict_info(result_pre)

    

