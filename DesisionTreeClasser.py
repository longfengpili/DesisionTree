#!/usr/bin/env python3
#-*- coding:utf-8 -*-

from sklearn.model_selection import GridSearchCV,train_test_split
from sklearn.tree import DecisionTreeClassifier,export_graphviz
from sklearn.metrics import make_scorer
from datetime import datetime
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
tree_log = logging.getLogger('tree')

class DecisionTreeClasser():
    def __init__(self):
        self.random_state = random_state
        self.dir = model_dir
        self.test_size = test_size

    def split_data(self,x,y):
        x_train,x_test,y_train,y_test = train_test_split(x,y,test_size=self.test_size)
        
        return x_train,x_test,y_train,y_test

    def grid_search_cv(self,x_train,y_train,param_grid,scoring='roc_auc',cv=5,verbose=0):
        start = datetime.now()
        kinds = np.prod([len(i) for i in param_grid.values()])
        tree_log.info('开始时间{},共计{}种'.format(start,kinds))
        
        dtc = DecisionTreeClassifier(random_state=self.random_state)

        grid_search = GridSearchCV(estimator=dtc,param_grid=param_grid,scoring=scoring,
                                return_train_score=True,cv=cv,verbose=verbose)
                
        grid_search.fit(x_train,y_train)
        
        end = datetime.now()
        
        seconds =(end - start).seconds
        tree_log.info('grid_search_cv,共计{}种，用时{}秒,base_params{}'.format(kinds,seconds,grid_search.best_params_))
        
        return grid_search.cv_results_,grid_search.best_params_

    def grid_search_result_top(self,cv_results,n=5):
        results = DataFrame(cv_results)[['params','rank_test_score','mean_test_score','mean_train_score','std_test_score',
                                        'std_train_score']]
        results.sort_values(by='rank_test_score',inplace=True)
        
        return results[:n]

    def desision_fit(self,params,x_train,y_train,x_test,y_test):
        
        dtc_t = DecisionTreeClassifier(random_state=self.random_state,**(params))
        dtc_t.fit(x_train,y_train)

        all_train_score = dtc_t.score(x_train,y_train)
        all_test_score = dtc_t.score(x_test,y_test)
        all_score_diff = abs(all_train_score - all_test_score)

        fit_result = Series([all_train_score,all_test_score,all_score_diff],index=['all_train_score','all_test_score','all_score_diff'])

        return fit_result
        
    def save_best_model(self,best_params,x_train,y_train,class_name=None,feature_names=None,**kw):

        dtc_t = DecisionTreeClassifier(random_state=self.random_state,**(best_params))
        dtc_t.fit(x_train,y_train)

        s = datetime.now().strftime("%Y-%m-%d[%H%M%S]")
        
        if not os.path.exists(self.dir):
            os.mkdir(self.dir)
        
        dot_data = export_graphviz(dtc_t,class_names=class_name,feature_names=feature_names,out_file=None,
                        rounded=True,filled=True,special_characters=True,**kw)
        graph = pydotplus.graph_from_dot_data(dot_data)
        graph.write_png(self.dir + s + '.png')
        file = self.dir + s + '.model'
        pickle.dump(dtc_t,open(file,'wb'))
        
    def load_recent_model(self):
        try:
            model_list = [i for i in os.listdir(self.dir) if '.model' in i]
            # print(model_list)
            recent_model = self.dir + '/' +  model_list[-1]
            recent_model = pickle.load(open(recent_model,'rb'))
            tree_log.info('load模型{}'.format(recent_model))
        except:
            tree_log.info('no model exists !')
            recent_model = None
        return recent_model
        

if __name__ == '__main__':
    tree = DecisionTreeClasser()
    tree.load_recent_model()


    # from sklearn.datasets import load_iris
    # iris = load_iris()
    # x = iris.data[:, :2]  # we only take the first two features.
    # y = iris.target
    # tree = DecisionTreeClasser()

    # x_train,x_test,y_train,y_test = tree.split_data(x,y)

    # param_grid = {
    #     'max_depth':np.arange(1,3,1)
    # }

    # results,best_params = tree.grid_search_cv(x_train,y_train,param_grid,scoring=None) #调参
    # tree.save_best_model(best_params,x_train,y_train)   #记录model
    # results_top = tree.grid_search_result_top(results,n=5) #参数前5
    # results_top_all_1 = results_top.apply(lambda x :tree.desision_fit(x.params,x_train,y_train,x_test,y_test),axis=1) #参数前5的测试情况
    # results_top_all = results_top.join(results_top_all_1) #合并
    # print(results_top)
    # print(results_top_all_1)
    # print(results_top_all)