#!/usr/bin/env python3
#-*- coding:utf-8 -*-

from DesisionTreeClasser import DecisionTreeClasser
from redshift import db_redshift
from DesisionTree import *
from datetime import datetime,date,timedelta
from pandas import DataFrame,Series

class TreeRun(DecisionTreeClasser,db_redshift):

    def __init__(self):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dir = mode_dir
        self.random_state = random_state
        self.test_size = test_size

    def update_retention_data(self,sql,date,app_name,puzzle_language,days):

        rows,result = self.redshift_execute(sql,date=date,app_name=app_name,puzzle_language=puzzle_language,day=days)

        return rows,result
        
    def predict_user_retention_status(self,sql,date,app_name,puzzle_language,days):
        rows,result = self.update_retention_data(sql,date,app_name,puzzle_language,days) #更新数据

        recent_model = self.load_recent_model()

        result = DataFrame(result)

        print(result)


    



if __name__ == '__main__':
    # with open('./sqls/retention.sql','r',encoding='utf-8') as f:
    #     sql = f.read()

    sql = 'select * from report_word.user_info_before7_after7 limit 10'
    tree_run = TreeRun()
    date = date(2018,10,1)
    tree_run.predict_user_retention_status(sql,date,app_name,puzzle_language,days)

