#!/usr/bin/env python3
#-*- coding:utf-8 -*-

import psycopg2
from datetime import date,timedelta,datetime
import re
import sys

sys.path.append('..')
import Tree_setting as tsetting

import pandas as pd
from pandas import DataFrame

import logging
from logging import config
from DesisionTree import *

config.fileConfig('treelog.conf')
redshift_log = logging.getLogger('redshift')

class db_redshift():
    def __init__(self):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def __redshift_connect(self):
        try:
            redshift_connection = psycopg2.connect(database=self.database,user=self.user,password=self.password,host=self.host,port=self.port)
        except Exception as e:
            redshift_log.error(e)
            raise('error,{}'.format(e))
        return redshift_connection

    def change_sql(self,sql,**kw):
        dict = {}
        for k,v in kw.items():
            dict['${}'.format(k)] = v
        for i in dict.keys():
            sql = re.sub('\{}'.format(i),"\'{}\'".format(dict[i]),sql)
        sql = re.sub('\$.*?,','null,',sql)
        sql = re.sub('\$.*?\)','null)',sql)
        sql = re.sub('\$.*? ','null ',sql)
        # redshift_log.info(sql)
        return sql

    def redshift_execute(self,sql,**kw):
        change_sql = self.change_sql(sql,**kw)
        rows = 0
        result = None

        conn = self.__redshift_connect()
        cur = conn.cursor()
        try:
            cur.execute(change_sql)
            rows = cur.rowcount
            if rows == -1:
                result = None
            else:
                result = cur.fetchall()
            conn.commit()
        except Exception as e:
            conn.rollback()
            redshift_log.error(e)
            redshift_log.info(change_sql)
            result = e
        conn.close()

        return rows,result

if __name__ == '__main__':
    redshift = db_redshift()
    with open('./sqls/retention.sql','r',encoding='utf-8') as f:
        sql = f.read()

    start_ts = date(2018,10,1)
    end_ts = date.today() - timedelta(days=7)
    while start_ts <= end_ts:
        rows,result = redshift.redshift_execute(sql,app_name=app_name,puzzle_language=puzzle_language,day=days,date=start_ts)
        print(start_ts,rows)
        start_ts += timedelta(days=1)
    # print(result)

