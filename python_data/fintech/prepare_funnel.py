# encoding: utf-8
# T跑T-1的funnel和regist数据
from __future__ import division
import sys
import os
import time
import datetime
import pandas as pd
import numpy as np
CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
ADD_PATH = "%s/../"%(CURRENT_DIR)
sys.path.append(ADD_PATH)

from layers.download_mysql_data import MysqlParser

d1 = datetime.datetime.today() + datetime.timedelta(days = -1)
d2 = datetime.datetime.today()

st_year = str(d1.year)
st_month = '0' + str(d1.month) if d1.month < 10 else str(d1.month)
st_day = '0' + str(d1.day) if d1.day < 10 else str(d1.day)
st = st_year + st_month + st_day

ed_year = str(d2.year)
ed_month = '0' + str(d2.month) if d2.month < 10 else str(d2.month)
ed_day = '0' + str(d2.day) if d2.day < 10 else str(d2.day)
ed = ed_year + ed_month + ed_day

parser = MysqlParser()
df_funnel = parser.download_mysql_oneday('funnel', st, ed)
df_regist = parser.download_mysql_oneday('regist', st, ed)

