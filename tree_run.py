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
        result['coin_after'] = result['coin_after'].fillna(200)
        result['enjoy_status_m'] = result['enjoy_status'].apply(lambda x : -1 if pd.isna(x) else 1 if x == 'enjoy-yes' else 0)
        result['iap_status_m'] = result['first_iap_date'].apply(lambda x:0 if pd.isna(x) else 1)
        result['retention_status_m'] = result['retention_ts'].apply(lambda x:0 if pd.isna(x) else 1)

        result['coin_after'] = result['coin_after'].astype('int')
        result['install_date'] = result['install_date'].astype('str')
        result['retention_ts'] = result['retention_ts'].astype('str')

        result = result.dropna(subset=['level_max','load_days', 'challenge_gids','challenge_games', 'coin_after', 'enjoy_status_m', 'iap_status_m'])
        result = result.reset_index(drop=True)

        return result

        
    def predict_user_retention_status(self,data):
        '''
        1、load model
        2、change data
        3、predict
        '''
        recent_model = self.load_recent_model()
        # print(data[:5])
        result = self.modify_data(data)
        # print(result.index)
        
        x = result.loc[:,['level_max','load_days', 'challenge_gids','challenge_games', 'coin_after', 'enjoy_status_m', 'iap_status_m']].values
        result_pre = recent_model.predict(x)

        result_pre = pd.concat([result,Series(result_pre)],axis=1)
        result_pre.columns = list(result.columns) + ['retention_predict_m']

        return result_pre

    def update_predict_info(self,data_pre,n=10000):
        
        update_result = {}

        chunks = [data_pre[i: i + n] for i in range(0, len(data_pre), n)]
        
        for i in chunks:
            sql_list = []
            for j in i.iterrows():
                sql = '''
                    update report_word.user_info_before7_after7
                    set retention_predict = {}
                    where user_id = '{}'
                    and app_name = '{}' and platform = '{}'
                    and puzzle_language = '{}'
                '''.format(j[1].retention_predict_m,j[1].user_id,j[1].app_name,
                            j[1].platform,j[1].puzzle_language)

                update_result.setdefault(j[1].install_date,0)
                update_result[j[1].install_date] += 1
                sql_list.append(sql)

            sql_update = ';'.join(sql_list)
            rows = self.redshift_update(sql_update)
            run_log.info('更新{}条记录'.format(len(i)))

        run_log.info('更新{}条记录'.format(update_result))

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

        score = (result[(0,0)] + result[(1,1)]) /result.sum()
        right_in_predictwrong = result[(1,0)] /(result[(0,0)] + result[(1,0)] )

        return score,right_in_predictwrong

    def main(self,date=None):
        '''
        1、计算前14日的数据
        2、计算分数，如果分数超过阀值，则根据前2个月数据重跑模型
        '''
        if date == None:
            date = datetime.date.today()
            date = date - timedelta(days=15)

        with open('./sqls/retention.sql','r',encoding='utf-8') as f:
            sql = f.read()

        # sql = 'select * from report_word.user_info_before7_after7 limit 1000'

        run_log.info('开始更新数据{}'.format(date))
        rows,result = self.update_retention_data(sql,date) #更新数据
        run_log.info('更新数据结束，更新{}条数据'.format(rows))
        run_log.info('开始预测数据')
        result_pre = self.predict_user_retention_status(result) #预测数据
        run_log.info('预测结束,预测数据数量为{}'.format(len(result_pre)))
        run_log.info('开始更新预测数据')
        update_result = self.update_predict_info(result_pre) #更新预测数据
        run_log.info('更新预测数据结束，结果{}'.format(update_result))
        run_log.info('开始计算score、right_in_predictwrong')
        score,right_in_predictwrong = self.model_predict_score(date) #计算score、right_in_predictwrong

        if score <= 0.8 and right_in_predictwrong >= 0.1: #重新构建模型
            run_log.info('重构模型，前模型score为{},right_in_predictwrong为{}'.format(score,right_in_predictwrong))
            start_date = date - timedelta(days=56)
            end_date = date - timedelta(days=1)
            sql = '''
                select *
                from report_word.user_info_before7_after7
                where install_date >= '{}' and install_date <= '{}'
                and app_name = '{}' and puzzle_language ='{}'
                ;
            '''.format(start_date,end_date,self.app_name,self.puzzle_language)

            rows,sql_result = self.redshift_select(sql)
            result_for_fit = self.modify_data(sql_result)

            run_log.info('重构模型，模型数据记录{},{}条'.format(rows,len(result_for_fit)))

            x = result_for_fit.loc[:,['level_max','load_days', 'challenge_gids','challenge_games', 'coin_after', 'enjoy_status_m', 'iap_status_m']].values
            y = result_for_fit.loc[:,'retention_status_m'].values

            x_train,x_test,y_train,y_test = self.split_data(x,y)

            tree = DecisionTreeClasser()
            param_grid = {'max_depth': np.arange(4,5),
                        'max_leaf_nodes': np.arange(60,80,10),
                        'min_samples_split': np.arange(0.01,0.1,0.02)
                        }
            _,best_params = tree.grid_search_cv(x_train,y_train,param_grid,scoring='roc_auc') #调参
            fit_result = self.desision_fit(best_params,x_train,y_train,x_test,y_test) #测试
            run_log.info('重构模型，新模型result{}'.format(fit_result))

            tree.save_best_model(best_params,x_train,y_train)   #记录model

            run_log.info('开始预测数据')
            result_pre = self.predict_user_retention_status(result) #预测数据
            run_log.info('预测结束,预测数据数量为{}'.format(len(result_pre)))
            run_log.info('开始更新预测数据')
            update_result = self.update_predict_info(result_pre) #更新预测数据
            run_log.info('更新预测数据结束，结果{}'.format(update_result))
            run_log.info('开始计算score、right_in_predictwrong')
            score,right_in_predictwrong = self.model_predict_score(date) #计算score、right_in_predictwrong

        run_log.info('score为{},right_in_predictwrong为{}'.format(score,right_in_predictwrong))

        return score,right_in_predictwrong





if __name__ == '__main__':

    tree_run = TreeRun()
    date = date(2018,10,15)
    tree_run.main(date=date)

    # fpath = r'C:\Users\chunyang.xu\Google 云端硬盘\桌面备份\2018年9月28日-word_data\word_v1_retention.csv'
    # with open(fpath,'r',encoding='utf-8') as f:
    #     sql_result = pd.read_csv(f,sep='|')
    # result_for_fit = tree_run.modify_data(sql_result)
    # print(result_for_fit[:5])
    # run_log.info('重构模型，模型数据记录{}条'.format(len(result_for_fit)))
    # x = result_for_fit.loc[:,['level_max','load_days', 'challenge_gids','challenge_games', 'coin_after', 'enjoy_status_m', 'iap_status_m']].values
    # y = result_for_fit.loc[:,'retention_status_m'].values
    # x_train,x_test,y_train,y_test = tree_run.split_data(x,y)
    # print(len(x_train),len(x_test),len(y_train),len(y_test))


    # sql = 'select * from report_word.user_info_before7_after7 limit 12'
    # run_log.info('开始更新数据{}'.format(date))
    # rows,result = tree_run.update_retention_data(sql,date) #更新数据
    # print(result[:5])
    # run_log.info('开始预测数据')
    # result_pre = tree_run.predict_user_retention_status(result) #预测数据
    # print(result_pre[:5])
    # run_log.info('开始更新预测数据')
    # update_result = tree_run.update_predict_info(result_pre,n=10) #更新预测数据
    # run_log.info('开始计算score、right_in_predictwrong')
    # score,right_in_predictwrong = tree_run.model_predict_score(date) #计算score、right_in_predictwrong
    # run_log.info('score为{},right_in_predictwrong为{}'.format(score,right_in_predictwrong))
    
