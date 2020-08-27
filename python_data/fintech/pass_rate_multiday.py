# encoding: utf-8
from __future__ import division
import sys
import os
import time
import datetime
import pandas as pd
import numpy as np
import math
import ast
CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
ADD_PATH = "%s/../"%(CURRENT_DIR)
sys.path.append(ADD_PATH)

from tools.mail import MyEmail
from tools.html import html_with_style
DATA_PATH = "%s/../data/basic_matrix" % (CURRENT_DIR)

def pass_rate(df):
    if len(df) == 0:
        return pd.DataFrame(data=[['零完成单']], columns=['总计'])
    out = []
    out.append((len(df),len(df[df['suggestion']=='1']),100.0*len(df[df['suggestion']=='1'])/len(df)))
    if len(df[df['type']=='first']) == 0:
        out.append((0,0,0))
    else:
        out.append((len(df[df['type']=='first']),len(df[(df['suggestion']=='1')&(df['type']=='first')]),100.0*len(df[(df['suggestion']=='1')&(df['type']=='first')])/len(df[df['type']=='first'])))
    if len(df[df['type']=='regular']) == 0:
        out.append((0,0,0))
    else:
        out.append((len(df[df['type']=='regular']),len(df[(df['suggestion']=='1')&(df['type']=='regular')]),100.0*len(df[(df['suggestion']=='1')&(df['type']=='regular')])/len(df[df['type']=='regular'])))
    if len(df[df['type']=='again']) == 0:
        out.append((0,0,0))
    else:
        out.append((len(df[df['type']=='again']),len(df[(df['suggestion']=='1')&(df['type']=='again')]),100.0*len(df[(df['suggestion']=='1')&(df['type']=='again')])/len(df[df['type']=='again'])))
    frame = pd.DataFrame(out, index=['总通过率','新单通过率','续贷通过率','再次通过率'], columns=['订单申请数','通过数','通过率'])
    return frame

def baseline(df):
    send_str = ''
    if len(df) == 0:
        return pd.DataFrame(data=[['无新单']], columns=['总计'])
    out = []
    baseline_data = []
    baseline = df['baseline'].values.tolist()
    for i in baseline:
        temp = ast.literal_eval(i)
        if len(temp) != 0:
            baseline_data.extend(temp)
    if baseline_data == []:
        return pd.DataFrame(data=[['无baseline']], columns=['总计'])

    labels = set(baseline_data)
    for i in labels:
        out.append((baseline_data.count(i),100.0*baseline_data.count(i)/len(df)))
    frame = pd.DataFrame(out, index=labels, columns=['个数','拒绝率'])
    # frame.set_index('baseline', inplace = True)
    return frame

def score(df):
    send_str = ''
    if len(df) == 0:
        return pd.DataFrame(data=[['无新单']], columns=['总计'])

    out = []
    temp = df['score'].values.tolist()
    score = []
    for i in temp:
        try:
            score.append(int(i))
        except:
            continue
    score = np.array(score)
    labels = ['>1000','901-1000','801-900','701-800','601-700','501-600','401-500','301-400','201-300','101-200','0-100','-0']
    out.append((sum(np.array(score)>1000),100.0*sum(np.array(score)>1000)/len(np.array(score))))
    out.append(((sum(np.array(score)>900)-sum(np.array(score)>1000)),100.0*(sum(np.array(score)>900)-sum(np.array(score)>1000))/len(np.array(score))))
    out.append(((sum(np.array(score)>800)-sum(np.array(score)>900)),100.0*(sum(np.array(score)>800)-sum(np.array(score)>900))/len(np.array(score))))
    out.append(((sum(np.array(score)>700)-sum(np.array(score)>800)),100.0*(sum(np.array(score)>700)-sum(np.array(score)>800))/len(np.array(score))))
    out.append(((sum(np.array(score)>600)-sum(np.array(score)>700)),100.0*(sum(np.array(score)>600)-sum(np.array(score)>700))/len(np.array(score))))
    out.append(((sum(np.array(score)>500)-sum(np.array(score)>600)),100.0*(sum(np.array(score)>500)-sum(np.array(score)>600))/len(np.array(score))))
    out.append(((sum(np.array(score)>400)-sum(np.array(score)>500)),100.0*(sum(np.array(score)>400)-sum(np.array(score)>500))/len(np.array(score))))
    out.append(((sum(np.array(score)>300)-sum(np.array(score)>400)),100.0*(sum(np.array(score)>300)-sum(np.array(score)>400))/len(np.array(score))))
    out.append(((sum(np.array(score)>200)-sum(np.array(score)>300)),100.0*(sum(np.array(score)>200)-sum(np.array(score)>300))/len(np.array(score))))
    out.append(((sum(np.array(score)>100)-sum(np.array(score)>200)),100.0*(sum(np.array(score)>100)-sum(np.array(score)>200))/len(np.array(score))))
    out.append(((sum(np.array(score)>0)-sum(np.array(score)>100)),100.0*(sum(np.array(score)>0)-sum(np.array(score)>100))/len(np.array(score))))
    out.append((sum(np.array(score)<=0),100.0*sum(np.array(score)<=0)/len(np.array(score))))
    frame = pd.DataFrame(out, index=labels, columns=['个数','占比'])
    return frame

def epoch_score(df):
    if len(df) == 0:
        return pd.DataFrame(data=[['无新单']], columns=['总计'])
    out = []
    temp = df['epoch_score'].values.tolist()
    score = []
    for i in temp:
        try:
            score.append(int(i))
        except:
            continue
    score = np.array(score)
    labels = ['>680','661-680','641-660','621-640','591-620','561-590','531-560','0-530','-0']
    out.append((sum(np.array(score)>680),100.0*sum(np.array(score)>680)/len(np.array(score))))
    out.append(((sum(np.array(score)>660)-sum(np.array(score)>680)),100.0*(sum(np.array(score)>660)-sum(np.array(score)>680))/len(np.array(score))))
    out.append(((sum(np.array(score)>640)-sum(np.array(score)>660)),100.0*(sum(np.array(score)>640)-sum(np.array(score)>660))/len(np.array(score))))
    out.append(((sum(np.array(score)>620)-sum(np.array(score)>640)),100.0*(sum(np.array(score)>620)-sum(np.array(score)>640))/len(np.array(score))))
    out.append(((sum(np.array(score)>590)-sum(np.array(score)>620)),100.0*(sum(np.array(score)>590)-sum(np.array(score)>620))/len(np.array(score))))
    out.append(((sum(np.array(score)>560)-sum(np.array(score)>590)),100.0*(sum(np.array(score)>560)-sum(np.array(score)>590))/len(np.array(score))))
    out.append(((sum(np.array(score)>530)-sum(np.array(score)>560)),100.0*(sum(np.array(score)>530)-sum(np.array(score)>560))/len(np.array(score))))
    out.append(((sum(np.array(score)>0)-sum(np.array(score)>530)),100.0*(sum(np.array(score)>0)-sum(np.array(score)>530))/len(np.array(score))))
    out.append((sum(np.array(score)<=0),100.0*sum(np.array(score)<=0)/len(np.array(score))))
    frame = pd.DataFrame(out, index=labels, columns=['个数','占比'])
    # frame.set_index('分数段', inplace = True)
    return frame

def read_basic_matrix(f):
    b = []
    for i in f.readlines():
        b.append(i[0:len(i)-1].split('|'))
    df = pd.DataFrame(data=b[1:],columns=b[0])
    df = df[df['type'] != 'nan']
    return df

if __name__ == '__main__':
    day1 = str(datetime.datetime.today() - datetime.timedelta(days=1))[0:10]
    day2 = str(datetime.datetime.today() - datetime.timedelta(days=2))[0:10]

    #load data
    with open(DATA_PATH+'/basic_matrix.'+day1) as a1:
        df1 = read_basic_matrix(a1)

    with open(DATA_PATH+'/basic_matrix.'+day2) as a2:
        df2 = read_basic_matrix(a2)

    ret1 = pd.concat([pass_rate(df1), pass_rate(df2)], keys = [day1, day2], axis = 1)
    ret1.fillna('')
    ret1 = pd.concat([ret1], keys = ['机审通过率'], axis = 1)

    df1 = df1[df1['type'] == 'first']
    df2 = df2[df2['type'] == 'first']
    ret2 = pd.concat([baseline(df1), baseline(df2)], keys = [day1, day2], axis = 1)
    ret2.fillna(0, inplace=True)
    ret2 = pd.concat([ret2], keys = ['新单baseline拒绝率'], axis = 1)
    ret3 = pd.concat([score(df1), score(df2)], keys = [day1, day2], axis = 1)
    ret3.fillna(0, inplace=True)
    ret3 = pd.concat([ret3], keys = ['新单score分布'], axis = 1)
    ret4 = pd.concat([epoch_score(df1), epoch_score(df2)], keys = [day1, day2], axis = 1)
    ret4.fillna(0, inplace=True)
    ret4 = pd.concat([ret4], keys = ['新单epoch_score分布'], axis = 1)

    send_str = ''
    send_str += html_with_style(ret1) + '<br>'
    send_str += html_with_style(ret2) + '<br>'
    send_str += html_with_style(ret3) + '<br>'
    send_str += html_with_style(ret4) + '<br>'

    mail = MyEmail()
    to_list = ['panyunxia@hinterstellar.com']
    # to_list = ['zhouchong@hinterstellar.com','panyunxia@hinterstellar.com','sundongxiang@hinterstellar.com']
    title = '通过率日环比'+day1
    mail.sendemail(send_str, title , to_list)






