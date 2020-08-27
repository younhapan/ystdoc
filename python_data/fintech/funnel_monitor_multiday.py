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
max_day = 29
day_list = []
day1 = str(datetime.datetime.today() - datetime.timedelta(days=1))[0:10]
day2 = str(datetime.datetime.today() - datetime.timedelta(days=2))[0:10]

# load data
df_regist_1 = pd.read_csv(DATA_PATH+'/regist.'+day1, encoding = 'utf-8')
df_funnel_1 = pd.read_csv(DATA_PATH+'/funnel.'+day1, encoding = 'utf-8')
df_1 = pd.merge(df_regist_1, df_funnel_1, how = 'left', on = 'create_date')

df_regist_2 = pd.read_csv(DATA_PATH+'/regist.'+day2, encoding = 'utf-8')
df_funnel_2 = pd.read_csv(DATA_PATH+'/funnel.'+day2, encoding = 'utf-8')
df_2 = pd.merge(df_regist_2, df_funnel_2, how = 'left', on = 'create_date')

df = pd.concat([df_1, df_2], axis = 0)
df.columns = ['日期', '注册人数', '申请人数', '机审通过人数', '人审通过人数', '人审拒绝人数', '放款人数']
df['C1'] = df['申请人数']/df['注册人数']
df['C2'] = df['机审通过人数']/df['申请人数']
df['C3'] = df['人审通过人数']/df['机审通过人数']
df['C4'] = df['放款人数']/df['人审通过人数']
df.replace(np.inf, 0, inplace = True)
df.fillna(0, inplace = True)
df[['C1','C2','C3','C4']] = df[['C1','C2','C3','C4']].applymap(lambda x: ('%.2f%%') %(x*100))

# df = df[['日期','注册人数','C1','申请人数','C2','机审通过人数','C3','人审通过人数','人审拒绝人数','C4','放款人数']].T
df = df[['日期','注册人数','C1','申请人数','C2','机审通过人数','C3','人审通过人数','人审拒绝人数','C4','放款人数']]
df.set_index('日期', inplace=True)
df = df.T

send_str += html_with_style(df) + '<br>'

mail = MyEmail()
to_list = ['panyunxia@hinterstellar.com']
title = '订单漏斗监控'+day1
mail.sendemail(send_str, title , to_list)






