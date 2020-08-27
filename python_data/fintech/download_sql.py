-*- coding: utf-8 -*-
from __future__ import division
import sys
import os
import collections
import datetime
import pandas as pd
import pymysql
CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))

MYSQL_HOST = "p2pdb.culfyfk0zivg.ap-south-1.rds.amazonaws.com"
MYSQL_USER = "dev"
MYSQL_PW = "Mrcd@1234567"
MYSQL_DATABASE = "india_product"


class MysqlParser(object):
    def __init__(self):
        self.today = datetime.datetime.today()
        self.year = str(self.today.year)
        self.month = '0' + str(self.today.month) if self.today.month < 10 else str(self.today.month)
        self.day = '0' + str(self.today.day) if self.today.day < 10 else str(self.today.day)
        self.DATA_PATH = "%s/../data/mysql" % (CURRENT_DIR)
        self.sql_dict = {
            'cl_borrow':{
            'columns': ['id','user_id','order_no','amount','real_amount','fee','create_time','state','interest'],
            'sql': """
                select
                    id,user_id,order_no,amount,real_amount,fee,create_time,state,interest
                from cl_borrow where create_time >='%s' and create_time <'%s'
                """,
            },
            'funnel':{
            'columns': ['create_date','apply_user','auto_passed','manual_passed','manual_failed','loan_success'],
            'sql': """
                SELECT date(create_time) create_date,
                   COUNT(DISTINCT CASE WHEN state = 10 THEN user_id END) apply_user,
                   COUNT(DISTINCT CASE WHEN state = 22 THEN user_id END) auto_passed,
                   COUNT(DISTINCT CASE WHEN state = 26 THEN user_id END) manual_passed,
                   COUNT(DISTINCT CASE WHEN state = 27 THEN user_id END) manual_failed,
                   COUNT(DISTINCT CASE WHEN state = 30 THEN user_id END) loan_success
                FROM cl_borrow_progress
                WHERE create_time >= '%s' AND create_time < '%s' GROUP BY date(create_time)
                """,
            },
            'regist':{
            'columns': ['create_date','regist_cnt'],
            'sql':"""
                SELECT date(regist_time) create_date, COUNT(DISTINCT uuid) regist_cnt FROM cl_user
                WHERE regist_time >= '%s' AND regist_time < '%s' GROUP BY date(regist_time)
                """,
            },
            'overdue':{
            'columns': ['auto_repay_time','borrow_id','finish_repay_time','time_diff','create_time'],
            'sql':"""
                SELECT t1.repay_time auto_repay_time, t1.borrow_id, t2.repay_time finish_repay_time,
                    timestampdiff(second, t1.repay_time, CASE WHEN t2.repay_time IS NULL THEN now() ELSE t2.repay_time END) time_diff,
                    date(t3.create_time) create_time
                FROM cl_borrow_repay t1
                LEFT JOIN cl_borrow_repay_log t2 ON t1.borrow_id = t2.borrow_id
                LEFT JOIN cl_borrow t3 ON t1.borrow_id = t3.id
                WHERE t1.repay_time >= '%s' AND t1.repay_time < '%s'
                """
            }
        }

    def db_connect(self):
        param = {
            'host': MYSQL_HOST,
            'user': MYSQL_USER,
            'password': MYSQL_PW,
            'database': MYSQL_DATABASE,
        }
        dbconn = pymysql.connect(**(param))
        return dbconn

    def read_sql_chunked(self, sqlcmd, conn, chunksize=100000, output=True):
        start = 0
        dfs = []
        if output:
            while True:
                print(sqlcmd)
                sql_str = "%s LIMIT %s OFFSET %s" % (sqlcmd, chunksize, start)
                print('LIMIT %s OFFSET %s' % (chunksize, start))
                df = pd.read_sql(sql_str, conn)
                start += chunksize
                dfs.append(df)
                if len(dfs[-1]) < chunksize:
                    break
        return pd.concat(dfs, ignore_index=True)

    def parse(self,start_day = None, end_day = None, type1 = 'cl_borrow'):
        if start_day == None or end_day == None or type1 == None:
            print(start_day,end_day,type1)
            return
        print(start_day,end_day)
        dbconn = self.db_connect()
        sqlcmd = self.sql_dict[type1]['sql'] % (str(start_day)[:19], str(end_day)[:19])
        print(len(self.sql_dict[type1]['columns']))
        df = self.read_sql_chunked(sqlcmd, dbconn)
        df.columns = self.sql_dict[type1]['columns']
        return df

    #download oneday data
    #默认下载当天，cl_borrow
    #start_day格式:'20200715'
    def download_mysql_oneday(self,type1=None,start_day=None,end_day=None,DATA_PATH=None):
        if start_day is None or end_day is None:
            start_day = str(self.year)+str(self.month)+str(self.day)
            temp = self.today + datetime.timedelta(days=1)
            temp_year = str(temp.year)
            temp_month = '0' + str(temp.month) if temp.month < 10 else str(temp.month)
            temp_day = '0' + str(temp.day) if temp.day < 10 else str(temp.day)
            end_day = str(temp_year)+str(temp_month)+str(temp_day)
        if DATA_PATH is None: DATA_PATH = self.DATA_PATH
        if type1 is None: type1 = 'cl_borrow'
        df = self.parse(datetime.datetime.strptime(start_day, '%Y%m%d'),datetime.datetime.strptime(end_day, '%Y%m%d'),type1)
        df.to_csv(DATA_PATH+'/'+type1+'.'+start_day[0:4]+'-'+start_day[4:6]+'-'+start_day[6:8], index=False)


if __name__ == '__main__':
    parser = MysqlParser()
    try:
        start_day = str(sys.argv[1])
        end_day = str(sys.argv[2])
        type1 = sys.argv[3]
        df = parser.download_mysql_oneday(type1, start_day, end_day)
    except:
        df = parser.download_mysql_oneday('cl_borrow')