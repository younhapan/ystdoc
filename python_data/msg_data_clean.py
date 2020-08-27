# -*- coding: utf-8 -*-
import os
import re
import pandas as pd
from datetime import datetime, timezone, timedelta

data = []

with open('/Users/younhapan/Downloads/out.txt',encoding='utf-8',errors='ignore') as out_file:
	for line in out_file:
		line = re.sub('{|}|\[|\]|_private_chat|From_Account|To_Account','',line).split('MsgContent')
		if len(line) > 1:
			line0 = re.sub('"|:|MsgTimestamp|MsgSeq|MsgRandom','',line[0])
			line0 = line0.split(',')
			line0.append(line[1])
			data.append(line0)

df = pd.DataFrame(data)
df.columns = ['From_Account','To_Account','MsgTimestamp','MsgSeq','MsgRandom','miss1','miss2','MsgContent']
df[['From_Account','To_Account','MsgTimestamp']] = df[['From_Account','To_Account','MsgTimestamp']].astype(int)

india_date=[]

for ts in df['MsgTimestamp']:
	ut = datetime.utcfromtimestamp(ts)
	ut.replace(tzinfo = timezone.utc)
	india_dt = ut.astimezone(timezone(timedelta(hours = 5.5)))
	india_date.append(india_dt.strftime('%Y%m%d %H:%M:%S'))

df['india_date'] = pd.to_datetime(india_date)

df[['From_Account','To_Account','MsgTimestamp','india_date','MsgContent']].to_excel('/Users/younhapan/Downloads/msg_file.xlsx',index=False,encoding='utf-8')