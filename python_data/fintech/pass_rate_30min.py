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


def reviewing_data(df,send_str):
    if len(df) ==0:
        send_str += '零单' + '<br>'
        return send_str
    send_str += '审核中单量:'+str(len(df[(df['type']=='nan')])) + ' ' + '<br>'
    send_str += '审核中比例:'+str(100.0*len(df[(df['type']=='nan')])/len(df)) + ' ' + '<br>' + '<br>'
    return send_str


def pass_rate(df,send_str):
    if len(df) == 0:
        send_str += '零完成单' + '<br>'
        return send_str
    out = []
    concat = []
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
    concat.append(frame)
    concat = pd.concat(concat, keys=['机审通过率'], axis=1)
    send_str += html_with_style(concat) + '<br>'
    return send_str


def baseline(df,send_str):
    if len(df) == 0:
        send_str += '无新单' + '<br>'
        return send_str
    out = []
    concat = []
    baseline_data = []
    baseline = df['baseline'].values.tolist()
    for i in baseline:
        temp = ast.literal_eval(i)
        if len(temp) != 0:
            baseline_data.extend(temp)
    if baseline_data == []:
        send_str += '无baseline' + '<br>'
        return send_str

    for i in set(baseline_data):
        out.append((i,baseline_data.count(i),100.0*baseline_data.count(i)/len(df)))
    frame = pd.DataFrame(out, columns=['baseline','个数','拒绝率'])
    concat.append(frame)
    concat = pd.concat(concat, keys=['新单baseline拒绝率'], axis=1)
    send_str += html_with_style(concat) + '<br>'
    return send_str


def score(df,send_str):
    if len(df) == 0:
        send_str += '无新单' + '<br>'
        return send_str
    out = []
    concat = []
    temp = df['score'].values.tolist()
    score = []
    for i in temp:
        try:
            score.append(int(i))
        except:
            continue
    score = np.array(score)
    out.append(('>1000',sum(np.array(score)>1000),100.0*sum(np.array(score)>1000)/len(np.array(score))))
    out.append(('901-1000',(sum(np.array(score)>900)-sum(np.array(score)>1000)),100.0*(sum(np.array(score)>900)-sum(np.array(score)>1000))/len(np.array(score))))
    out.append(('801-900',(sum(np.array(score)>800)-sum(np.array(score)>900)),100.0*(sum(np.array(score)>800)-sum(np.array(score)>900))/len(np.array(score))))
    out.append(('701-800',(sum(np.array(score)>700)-sum(np.array(score)>800)),100.0*(sum(np.array(score)>700)-sum(np.array(score)>800))/len(np.array(score))))
    out.append(('601-700',(sum(np.array(score)>600)-sum(np.array(score)>700)),100.0*(sum(np.array(score)>600)-sum(np.array(score)>700))/len(np.array(score))))
    out.append(('501-600',(sum(np.array(score)>500)-sum(np.array(score)>600)),100.0*(sum(np.array(score)>500)-sum(np.array(score)>600))/len(np.array(score))))
    out.append(('401-500',(sum(np.array(score)>400)-sum(np.array(score)>500)),100.0*(sum(np.array(score)>400)-sum(np.array(score)>500))/len(np.array(score))))
    out.append(('301-400',(sum(np.array(score)>300)-sum(np.array(score)>400)),100.0*(sum(np.array(score)>300)-sum(np.array(score)>400))/len(np.array(score))))
    out.append(('201-300',(sum(np.array(score)>200)-sum(np.array(score)>300)),100.0*(sum(np.array(score)>200)-sum(np.array(score)>300))/len(np.array(score))))
    out.append(('101-200',(sum(np.array(score)>100)-sum(np.array(score)>200)),100.0*(sum(np.array(score)>100)-sum(np.array(score)>200))/len(np.array(score))))
    out.append(('0-100',(sum(np.array(score)>0)-sum(np.array(score)>100)),100.0*(sum(np.array(score)>0)-sum(np.array(score)>100))/len(np.array(score))))
    out.append(('-0',sum(np.array(score)<=0),100.0*sum(np.array(score)<=0)/len(np.array(score))))
    frame = pd.DataFrame(out, columns=['分数段','个数','占比'])
    concat.append(frame)
    concat = pd.concat(concat, keys=['新单score分布'], axis=1)
    send_str += html_with_style(concat) + '<br>'
    return send_str


def epoch_score(df,send_str):
    if len(df) == 0:
        send_str += '无新单' + '<br>'
        return send_str
    out = []
    concat = []
    temp = df['epoch_score'].values.tolist()
    score = []
    for i in temp:
        try:
            score.append(int(i))
        except:
            continue
    score = np.array(score)
    out.append(('>680',sum(np.array(score)>680),100.0*sum(np.array(score)>680)/len(np.array(score))))
    out.append(('661-680',(sum(np.array(score)>660)-sum(np.array(score)>680)),100.0*(sum(np.array(score)>660)-sum(np.array(score)>680))/len(np.array(score))))
    out.append(('641-660',(sum(np.array(score)>640)-sum(np.array(score)>660)),100.0*(sum(np.array(score)>640)-sum(np.array(score)>660))/len(np.array(score))))
    out.append(('621-640',(sum(np.array(score)>620)-sum(np.array(score)>640)),100.0*(sum(np.array(score)>620)-sum(np.array(score)>640))/len(np.array(score))))
    out.append(('591-620',(sum(np.array(score)>590)-sum(np.array(score)>620)),100.0*(sum(np.array(score)>590)-sum(np.array(score)>620))/len(np.array(score))))
    out.append(('561-590',(sum(np.array(score)>560)-sum(np.array(score)>590)),100.0*(sum(np.array(score)>560)-sum(np.array(score)>590))/len(np.array(score))))
    out.append(('531-560',(sum(np.array(score)>530)-sum(np.array(score)>560)),100.0*(sum(np.array(score)>530)-sum(np.array(score)>560))/len(np.array(score))))
    out.append(('0-530',(sum(np.array(score)>0)-sum(np.array(score)>530)),100.0*(sum(np.array(score)>0)-sum(np.array(score)>530))/len(np.array(score))))
    out.append(('-0',sum(np.array(score)<=0),100.0*sum(np.array(score)<=0)/len(np.array(score))))
    frame = pd.DataFrame(out, columns=['分数段','个数','占比'])
    concat.append(frame)
    concat = pd.concat(concat, keys=['新单epoch_score分布'], axis=1)
    send_str += html_with_style(concat) + '<br>'
    return send_str


if __name__ == '__main__':
    try:
        year = str(sys.argv[3])
        month = str(sys.argv[4])
        day = str(sys.argv[5])
    except:
        try:
            delta = int(sys.argv[1])
            today = datetime.datetime.today() + datetime.timedelta(days=delta)
            year = str(today.year)
            month = '0' + str(today.month) if today.month < 10 else str(today.month)
            day = '0' + str(today.day) if today.day < 10 else str(today.day)
        except:
            today = datetime.datetime.today()
            year = str(today.year)
            month = '0' + str(today.month) if today.month < 10 else str(today.month)
            day = '0' + str(today.day) if today.day < 10 else str(today.day)

    #laod data
    a = open(DATA_PATH+'/basic_matrix.'+year+'-'+month+'-'+day)
    b=[]
    #a = open('/data/risk/miracle2/data/basic_matrix/basic_matrix.2020-07-18')
    for i in a.readlines():
        b.append(i[0:len(i)-1].split('|'))

    #convert to dataframe
    target_col = ['user_id','suggestion','created_time','type','baseline','score','te','epoch_score']
    df = pd.DataFrame(data=b[1:],columns=b[0])
    send_str = ''
    send_str = reviewing_data(df,send_str)
    df = df[df['type']!='nan'] #delete reviewing data
    send_str = pass_rate(df,send_str)

    df = df[df['type']=='first']
    send_str = baseline(df,send_str)
    send_str = score(df,send_str)
    send_str = epoch_score(df,send_str)

    mail = MyEmail()
    to_list = ['zhouchong@hinterstellar.com','panyunxia@hinterstellar.com','sundongxiang@hinterstellar.com']
    try:
        title = str(sys.argv[2])+str(datetime.datetime.today())[:19]
    except:
        title = '通过率实时监控'+str(datetime.datetime.today())[:19]
    mail.sendemail(send_str, title , to_list)






