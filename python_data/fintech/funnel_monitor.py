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
today = datetime.datetime.today()
dt = today + datetime.timedelta(days = -1)
year = str(dt.year)
month = '0' + str(dt.month) if dt.month < 10 else str(dt.month)
day = '0' + str(dt.day) if dt.day < 10 else str(dt.day)

# load data
df_regist = pd.read_csv(DATA_PATH+'/regist.'+year+'-'+month+'-'+day, encoding = 'utf-8')
df_funnel = pd.read_csv(DATA_PATH+'/funnel.'+year+'-'+month+'-'+day, encoding = 'utf-8')
df = pd.merge(df_regist, df_funnel, how = 'left', on = 'create_date')
df.columns = ['日期', '注册人数', '申请人数', '机审通过人数', '人审通过人数', '人审拒绝人数', '放款人数']
df['C1'] = df['申请人数']/df['注册人数']
df['C2'] = df['机审通过人数']/df['申请人数']
df['C3'] = df['人审通过人数']/df['机审通过人数']
df['C4'] = df['放款人数']/df['人审通过人数']
df.replace(np.inf, 0, inplace = True)
df.fillna(0, inplace = True)
df[['C1','C2','C3','C4']] = df[['C1','C2','C3','C4']].applymap(lambda x: ('%.2f%%') %(x*100))

df = df[['日期','注册人数','C1','申请人数','C2','机审通过人数','C3','人审通过人数','人审拒绝人数','C4','放款人数']].T
df.columns = ['data']

if len(df) ==0:
    send_str += '无订单申请'
else:
    send_str += html_with_style(df) + '<br>'

mail = MyEmail()
to_list = ['panyunxia@hinterstellar.com']
title = '订单漏斗监控'+year+'-'+month+'-'+day
mail.sendemail(send_str, title , to_list)






