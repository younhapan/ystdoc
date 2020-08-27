# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import xlrd
import openpyxl

data = pd.read_excel('/Users/younhapan/Downloads/chat.xlsx',usecols=[0,1,2])
data['sender']=abs(data['sender'])
data['receiver']=abs(data['receiver'])

df=data.pivot_table(values=['msgtime'],index=['receiver','sender'],aggfunc='count').stack().reset_index()
df.rename(columns={'level_2':'cnt_name',0:'msg_cnt'},inplace=True)
# result=df[df['msg_cnt']>=5]
result=df
result.to_excel('/Users/younhapan/Downloads/yinyin_jianzhi.xlsx',index=False,encoding='utf-8')