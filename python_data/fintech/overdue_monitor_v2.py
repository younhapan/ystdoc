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
dt = today - datetime.timedelta(days = 1) - datetime.timedelta(days = 29)
year = str(dt.year)
month = '0' + str(dt.month) if dt.month < 10 else str(dt.month)
day = '0' + str(dt.day) if dt.day < 10 else str(dt.day)

def get_overdue_label(df):
    overdue_now = 0
    now_time = datetime.datetime.today()
    auto_repay_time = datetime.datetime.strptime(str(df['auto_repay_time'])[:19],'%Y-%m-%d %H:%M:%S')
    if pd.isnull(df['finish_repay_time']):
        if auto_repay_time < now_time:
            overdue_now = 1

    overdue = [0] * 7
    day_dict = ['0', '1', '2', '3', '7', '15', '30']

    delta_hours = df['time_diff'] / (60*60.0)
    if delta_hours > 0:
        overdue[1] = 1
    if delta_hours > 24*2:
        overdue[2] = 1
    if delta_hours > 24*3:
        overdue[3] = 1
    if delta_hours > 24*7:
        overdue[4] = 1
    if delta_hours > 24*15:
        overdue[5] = 1
    if delta_hours > 24*30:
        overdue[6] = 1
    overdue[0] = overdue_now

    data = []
    columns = ['auto_repay_time','borrow_id','finish_repay_time',]
    data.append(str(df['auto_repay_time'])[:10])
    data.append(df['borrow_id'])
    if pd.isnull(df['finish_repay_time']):
        data.append(1) # 截至现在未还订单
    else:
        data.append(0)

    for i in range(0, len(overdue)):
        data.append(overdue[i])
        columns.append('overdue_'+day_dict[i])
    return pd.Series(data,index=columns)

def agg_funcs(df):
    ret = df.groupby('auto_repay_time')[['borrow_id','finish_repay_time','overdue_0', 'overdue_1','overdue_2','overdue_3','overdue_7','overdue_15','overdue_30']].agg({'borrow_id':'nunique',\
         'finish_repay_time':'sum','overdue_0':'sum', 'overdue_1':'sum','overdue_2':'sum','overdue_3':'sum','overdue_7':'sum','overdue_15':'sum','overdue_30':'sum'})
    ret.reset_index(inplace = True)

    ret['overdue_1_rate'] = ret['overdue_1']/ret['borrow_id']
    ret['overdue_2_rate'] = ret['overdue_2']/ret['borrow_id']
    ret['overdue_3_rate'] = ret['overdue_3']/ret['borrow_id']
    ret['overdue_7_rate'] = ret['overdue_7']/ret['borrow_id']
    ret['overdue_15_rate'] = ret['overdue_15']/ret['borrow_id']
    ret['overdue_30_rate'] = ret['overdue_30']/ret['borrow_id']
    ret['overdue_0_rate'] = ret['overdue_0']/ret['borrow_id']

    ret = ret[['auto_repay_time','borrow_id','overdue_1','finish_repay_time','overdue_1_rate','overdue_2_rate','overdue_3_rate','overdue_7_rate','overdue_15_rate',\
                'overdue_30_rate','overdue_0_rate']]
    return ret

def cal_rate(df):
    now = datetime.datetime.today()
    now = datetime.datetime.strptime(str(now)[:10], '%Y-%m-%d')
    day_dicts = ['2','3','7','15','30']

    result_cate = []
    keys_cate = []

    for (idx,row) in df.iterrows():
        result = {}
        repay_label = row['auto_repay_time']
        result['repay_label'] = repay_label
        result['loan_size'] = row['borrow_id']
        result['delay1_size'] = row['overdue_1']
        result['norepay_size'] = row['finish_repay_time']
        result['overdue_1_rate'] = row['overdue_1_rate']

        for day_dict in day_dicts:
            max_day = datetime.datetime.strptime(str(repay_label)[0:10], '%Y-%m-%d')
            delta_day = float((now - max_day).days)
            if delta_day > float(day_dict):
                result['overdue_'+day_dict+'_rate'] = row['overdue_'+day_dict+'_rate']
            else:
                result['overdue_'+day_dict+'_rate'] = ''

        result['overdue_0_rate'] = row['overdue_0_rate']
        result_cate.append(result)

    final = pd.DataFrame(data = result_cate)
    final.columns=['应还日期','放款单数','首逾单数','未还单数','首日','2日','3日','7日','15日','30日','截至现在']
    final.replace(np.inf, 0, inplace = True)
    final[['首日','2日','3日','7日','15日','30日','截至现在']] = final[['首日','2日','3日','7日','15日','30日','截至现在']].applymap(lambda x: '%.2f%%' %(x*100) if x != '' else '')
    final.sort_index(ascending=False,inplace=True)
    return final

# load decision info data
def load_anubis_decision_info(dt, data_path = CURRENT_DIR):
    location = "%s/../data/info/anubis_decision_info.%s" % (data_path, dt)
    b = []
    info_data = []
    columns = ['borrow_id','type'] # 和mysql保持一致，订单id用borrow_id
    try:
        with open(location) as a:
            for i in a.readlines():
                b.append(eval(i)['content'])
    except:
        print('anubis_decision_info file raise error')
    for i in b:
        if i['suggestion'] == '1':
            info = []
            info.append(i.get('application_id','nan'))
            info.append(i.get('type','nan'))
            info_data.append(info)
    df = pd.DataFrame(data = info_data, columns = columns)
    return df

def type_str(df, type = 'all'):
    if type == 'all':
        df = df
    else:
        df = df[df['type'] == type]
        if len(df) == 0:
            return '该类型订单数量为0'
    df = df.apply(get_overdue_label, axis = 1)
    ret = agg_funcs(df)
    final = cal_rate(ret)
    send_str = html_with_style(final) + '<br>'
    return send_str

if __name__ == '__main__':
    title = sys.argv[1]
    df_overdue = pd.read_csv(DATA_PATH+'/overdue.'+year+'-'+month+'-'+day, encoding = 'utf-8')
    df_overdue['borrow_id'] = df_overdue['borrow_id'].apply(lambda x: str(x))
    send_str = ''
    if title == '分级用户逾期监控':
        # combine overdue data and decision info data
        create_date = set(df_overdue['create_time'].values.tolist())
        create_dates = []
        for d in create_date:
            d = datetime.datetime.strptime(d, '%Y-%m-%d')
            d1 = d + datetime.timedelta(days = 1)
            d2 = d - datetime.timedelta(days = 1)
            create_dates.extend([str(d)[:10], str(d1)[:10], str(d2)[:10]])
        create_dates = set(create_dates)

        all_info_data = []
        for dt in create_dates:
            all_info_data.append(load_anubis_decision_info(dt))
        df_info = pd.concat(all_info_data, axis = 0)
        df = pd.merge(df_overdue, df_info, how = 'left', on = 'borrow_id')
        # print(df)
        send_str += 'type = first' + '<br>'
        send_str += type_str(df, 'first')
        send_str += 'type = regular' + '<br>'
        send_str += type_str(df, 'regular')
        send_str += 'type = again' + '<br>'
        send_str += type_str(df, 'again')
    elif title == '订单逾期监控':
        send_str += type_str(df_overdue)

    mail = MyEmail()
    # to_list = ['panyunxia@hinterstellar.com']
    to_list = ['zhouchong@hinterstellar.com', 'panyunxia@hinterstellar.com']
    title = title + str(datetime.datetime.today()-datetime.timedelta(days = 1))[0:10]
    mail.sendemail(send_str, title , to_list)



