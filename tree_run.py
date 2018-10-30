#!/usr/bin/env python3
#-*- coding:utf-8 -*-

from DesisionTreeClasser import DecisionTreeClasser
from redshift import db_redshift
from setting import *
from datetime import datetime,date,timedelta
import datetime
from sklearn.model_selection import GridSearchCV,train_test_split
from sklearn.tree import DecisionTreeClassifier,export_graphviz
from sklearn.metrics import make_scorer
import pydotplus
import pickle
import os
import numpy as np
import pandas as pd
from pandas import Series,DataFrame
import logging
from logging import config
from setting import *

config.fileConfig('treelog.conf')
run_log = logging.getLogger('run')

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
        self.app_name = app_name
        self.puzzle_language = puzzle_language
        self.days = days

    def update_retention_data(self,sql,date):

        rows,result = self.redshift_select(sql,date=date,app_name=self.app_name,puzzle_language=self.puzzle_language,day=self.days)

        return rows,result

    def modify_data(self,sql_result):

        result_all = DataFrame(sql_result,columns=self.columns)
        result = result_all.copy()
        result['first_nation'] = result['first_nation'].fillna('unknown')
        result['coin_after_m'] = result['coin_after'].fillna(200)
        result['enjoy_status_m'] = result['enjoy_status'].apply(lambda x : -1 if pd.isna(x) else 1 if x == 'enjoy-yes' else 0)
        result['iap_status_m'] = result['first_iap_date'].apply(lambda x:0 if pd.isna(x) else 1)
        result['retention_status_m'] = result['retention_ts'].apply(lambda x:0 if pd.isna(x) else 1)

        result['coin_after'] = result['coin_after'].astype('int')
        result['install_date'] = result['install_date'].astype('str')
        result['retention_ts'] = result['retention_ts'].astype('str')

        return result

        
    def predict_user_retention_status(self,data):
        '''
        1、load model
        2、change data
        3、predict
        '''
        recent_model = self.load_recent_model()
        result = self.modify_data(data)

        run_log.info(result.columns)
        
        x = result.loc[:,['level_max','load_days', 'challenge_gids','challenge_games', 'coin_after_m', 'enjoy_status_m', 'iap_status_m',]].values
        result_pre = recent_model.predict(x)
        result_pre = pd.concat([result,Series(result_pre)],axis=1)
        result_pre.columns = result.columns + list('retention_predict')

        return result

        
    def predict_user_retention_status(self,data):
        '''
        1、load model
        2、change data
        3、predict
        '''
        recent_model = self.load_recent_model()
        result = self.modify_data(data)
        
        x = result.iloc[:,6:13].values
        result_pre = recent_model.predict(x)
        result_pre = pd.concat([result.drop(columns=['retention_predict']),Series(result_pre)],axis=1)
        result_pre.columns = ['user_id', 'app_name', 'platform', 'puzzle_language', 'install_date',
       'first_nation', 'level_max', 'load_days', 'challenge_gids','challenge_games', 'coin_after', 'enjoy_status',  'iap_status', 'retention_status','first_iap_date',
       'retention_ts', 'retention_predict']

        return result_pre

    def update_predict_info(self,date,data_pre):
        data_pre = data_pre.drop(columns=['iap_status', 'retention_status'])
        print(data_pre.dtypes)
        data = ','.join([str(tuple(i[1].values)) for i in data_pre.iterrows()])
        

        sql_delete = 'delete report_word.user_info_before7_after7 where install_date = {}'.format(date)
        rows,result = self.redshift_select(sql_delete)
        print(rows,result)

        sql_insert = '''
            insert into report_word.user_info_before7_after7
            (user_id, app_name, platform, puzzle_language, install_date,
            first_nation, level_max, load_days, challenge_gids,challenge_games, coin_after, enjoy_status,first_iap_date,
            retention_ts, retention_predict)
            values
            {}
        '''.format(data)

        rows,result = self.redshift_select(sql_insert)
        print(rows,result)

        return update_result
        
    def model_predict_score(self,date):
        sql = '''
            select retention_predict::integer,
            (case when retention_ts is null then 0 else 1 end) as retention_real
            from report_word.user_info_before7_after7
            where install_date = '{}'
            and app_name = '{}' and puzzle_language ='{}'
            and retention_predict is not null
        '''.format(date,self.app_name,self.puzzle_language)

        rows,result = self.redshift_select(sql)
        result = DataFrame(result,columns=['retention_predict','retention_real'])
        result = result.groupby(['retention_real','retention_predict'])['retention_predict'].count()

        for i in np.arange(0,2):
            for j in np.arange(0,2):
                if (i,j) not in list(result.index.values):
                    result[(i,j)] = 0

        print(result)
        score = (result[(0,0)] + result[(1,1)]) /result.sum()
        right_in_predictwrong = result[(1,0)] /(result[(0,0)] + result[(1,0)] )

        return score,right_in_predictwrong

    def main(self,run_date=None):
        '''
        1、计算前14日的数据
        2、计算分数，如果分数超过阀值，则根据前2个月数据重跑模型
        '''
        if run_date == None:
            run_date = datetime.date.today()

        date = run_date - timedelta(days=15)

        # with open('./sqls/retention.sql','r',encoding='utf-8') as f:
        #     sql = f.read()

        sql = 'select * from report_word.user_info_before7_after7 limit 100'

        run_log.info('开始更新数据{}'.format(date))
        rows,result = self.update_retention_data(sql,date) #更新数据
        run_log.info('开始预测数据')
        result_pre = self.predict_user_retention_status(result) #预测数据
        run_log.info('开始更新预测数据')
        update_result = self.update_predict_info(date,result_pre) #更新预测数据
        run_log.info('开始计算score、right_in_predictwrong')
        score,right_in_predictwrong = self.model_predict_score(date) #计算score、right_in_predictwrong

        while score <= 0.8 and right_in_predictwrong >= 0.1: #重新构建模型
            run_log.info('更新模型，前模型score为{},right_in_predictwrong为{}'.format(score,right_in_predictwrong))
            start_date = run_date - timedelta(days=28)
            end_date = run_date - timedelta(days=1)
            sql = '''
                select *
                from report_word.user_info_before7_after7
                where install_date >= '{}' and install_date <= '{}'
                and app_name = '{}' and puzzle_language ='{}'
                and retention_predict is not null
                limit 10;
            '''.format(start_date,end_date,self.app_name,self.puzzle_language)

            rows,sql_result = self.redshift_select(sql)
            result_for_fit = self.modify_data(sql_result)

            x = result_for_fit.iloc[:,6:13].values
            y = result_for_fit.iloc[:,13].values

            x_train,x_test,y_train,y_test = self.split_data(x,y)

            tree = DecisionTreeClasser()
            param_grid = {'max_depth': np.arange(4,14),
                        'max_leaf_nodes': np.arange(60,170,10),
                        'min_samples_split': np.arange(0.01,0.1,0.02)
                        }
            results,best_params = tree.grid_search_cv(x_train,y_train,param_grid,scoring=None) #调参
            tree.save_best_model(best_params,x_train,y_train)   #记录model

            score,right_in_predictwrong = self.model_predict_score(date) #计算score、right_in_predictwrong 根据新模型

        run_log.info('score为{},right_in_predictwrong为{}'.format(score,right_in_predictwrong))

        return score,right_in_predictwrong


            


        

        



if __name__ == '__main__':
    # with open('./sqls/retention.sql','r',encoding='utf-8') as f:
    #     sql = f.read()
    print(date.today())
    tree_run = TreeRun()
    sql = 'select * from report_word.user_info_before7_after7 limit 100'
    rows,result = tree_run.update_retention_data(sql,date) #更新数据
    
    result_pre = tree_run.predict_user_retention_status(result) #预测数据
