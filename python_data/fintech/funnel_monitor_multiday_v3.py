# encoding: utf-8
from __future__ import division
import sys
import os
import time
import datetime
import pandas as pd
import numpy as np
import math
CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
ADD_PATH = "%s/../"%(CURRENT_DIR)
sys.path.append(ADD_PATH)

from tools.mail import MyEmail
from tools.html import html_with_style
DATA_PATH = "%s/../data/mysql" % (CURRENT_DIR)

send_str = ''
# load data for max_day
def load_data(max_day):
    day_list = []
    for i in range(1, max_day):
        dt = str(datetime.datetime.today() - datetime.timedelta(days=i))[0:10]
        day_list.append(dt)
    df_dts = []
    for dt in day_list:
        try:
            df_regist_dt = pd.read_csv(DATA_PATH+'/regist.'+dt, encoding = 'utf-8')
        except:
            df_regist_dt = pd.DataFrame(data=[[dt, 0]], columns =  ['create_date','regist_cnt'])
        try:
            df_funnel_dt = pd.read_csv(DATA_PATH+'/funnel.'+dt, encoding = 'utf-8')
        except:
            df_funnel_dt = pd.DataFrame(data=[[dt,0,0,0,0,0]], columns =  ['create_date','apply_user','auto_passed','manual_passed','manual_failed','loan_success'])
        df_dt = pd.merge(df_regist_dt, df_funnel_dt, how = 'left', on = 'create_date')
        df_dts.append(df_dt)

    df = pd.concat(df_dts, axis = 0)
    df = df[['create_date', 'regist_cnt', 'apply_user', 'auto_passed', 'manual_passed', 'manual_failed', 'loan_success']] # mysql中数据有其他指标，只取特定几个
    df.columns = ['日期', '注册人数', '申请人数', '机审通过人数', '人审通过人数', '人审拒绝人数', '放款人数']
    df['C1'] = df['申请人数']/df['注册人数']
    df['C2'] = df['机审通过人数']/df['申请人数']
    df['C3'] = df['人审通过人数']/df['机审通过人数']
    df['C4'] = df['放款人数']/df['人审通过人数']
    df.replace(np.inf, 0, inplace = True)
    df.fillna(0, inplace = True)
    df[['C1','C2','C3','C4']] = df[['C1','C2','C3','C4']].applymap(lambda x: ('%.2f%%') %(x*100))
    df = df[['日期','注册人数','C1','申请人数','C2','机审通过人数','C3','人审通过人数','人审拒绝人数','C4','放款人数']]
    df = pd.DataFrame(data = df.iloc[:,1:].values.tolist(), index = df['日期'].values.tolist(), columns = df.columns.values.tolist()[1:])
    return df, day_list

if __name__ == '__main__':
    max_day = sys.argv[1]
    max_day = int(max_day)
    if max_day > 2:
        title = '订单漏斗汇总'
        df, day_list = load_data(max_day)
    elif max_day == 2:
        title = '订单漏斗监控'
        df, day_list = load_data(max_day)
        df = df.T

    mail = MyEmail()
    send_str += html_with_style(df) + '<br>'
    to_list = ['panyunxia@hinterstellar.com']
    # to_list = ['zhouchong@hinterstellar.com', 'panyunxia@hinterstellar.com']
    title = title + day_list[0]
    mail.sendemail(send_str, title , to_list)






