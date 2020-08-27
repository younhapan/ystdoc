# coding: utf-8
import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
from  packer import *

def get_overdue_label(row):
    overdue_now = 0
    delta_hours = 0
    now_time = datetime.datetime.today()
    auto_repay_time = datetime.datetime.strptime(str(row['auto_repay_time'])[:19],'%Y-%m-%d %H:%M:%S')
    if pd.isnull(row['finish_repay_time']):
        if auto_repay_time < now_time:
            overdue_now = 1
        finish_repay_time = now_time
    else:
        finish_repay_time = datetime.datetime.strptime(str(row['finish_repay_time'])[:19],'%Y-%m-%d %H:%M:%S')
    delta_hours = (finish_repay_time - auto_repay_time).days * 24.0 + (finish_repay_time - auto_repay_time).seconds / (60 * 60.0)
    overdue = [0] * 7
    day_dict = ['0', '1', '2', '3', '7', '15', '30']
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
    columns = ['ca_id']
    cate = str(row['cate'])
    data.append(row['ca_id'])
    data.append(row['cate'])
    data.append(str(row['created_time'])[:10])
    data.append(str(row['auto_repay_time'])[:19])
    if pd.isnull(row['finish_repay_time']):
        data.append('')
    else:
        data.append(str(row['finish_repay_time'])[:19])

    for i in ['cate','created_time','auto_repay_time','finish_repay_time']:
        columns.append(str(cate)+'_'+i)

    for i in range(0, len(overdue)):
        data.append(overdue[i])
        columns.append(str(cate)+'_overdue_'+day_dict[i])
    return pd.Series(data,index=columns)


def get_first_auto_repay_time(df,cates):
    logging.info('---SATART Get_first_auto_repay_time')
    clock()

    overdue_list = []
    for cate in tqdm(cates):
        print '---cate ', cate
        filted_df = df[(df['cate'] == cate)]
        overdue_df = filted_df.apply(get_overdue_label, axis=1)
        overdue_list.append(overdue_df)
    for i in tqdm(range(0, len(overdue_list))):
        print 'overdue_list = ', i
        if i == 0:
            df = overdue_list[0]
        else:
            df_new = overdue_list[i]
            df = pd.merge(df_new, df, on='ca_id', how='outer')
    df['first_time']=df['0_auto_repay_time']

    logging.info('...END')
    clock()
    return df


def get_week_split(start_day=None, end_day=None, gap=7, cate=0):
    logging.info('...START Get_week_split %s - %s' % (start_day, end_day))
    clock()

    start_day = datetime.datetime.strptime(str(start_day)[:10], '%Y-%m-%d') - datetime.timedelta(days=1)
    end_day = datetime.datetime.strptime(str(end_day)[:10], '%Y-%m-%d')

    #每周六发送，上周六-这周日 0-6:周一到周日
    while start_day.weekday() !=5:
        start_day = start_day + datetime.timedelta(days=1)
    day_list = []
    day_list.append(start_day)
    temp_day = start_day
    while temp_day + datetime.timedelta(days=gap) <= end_day:
        temp_day = temp_day + datetime.timedelta(days=gap)
        day_list.append(temp_day)
    result = []
    for i in range(len(day_list)-1):
        result.append((day_list[i], day_list[i+1]-datetime.timedelta(days=1)))
    day_list_new = []
    for (s,e) in result:
        s = str(s)[:10]
        e = str(e)[:10]
        day_list_new.append((s,e))

    logging.info('...END Get_week_split')
    clock()
    return day_list_new

def get_week_label(row,day_list,day,cate=0):
    #logging.info('...START Ge_week_label')
    #clock()

    day_delta1 = datetime.datetime.strptime(str(row[day])[:10], '%Y-%m-%d') - datetime.timedelta(days=1) - relativedelta(months=int(cate))
    day_delta1 = str(day_delta1)[:10]
    label=-1
    for (s,e) in day_list:
        if day_delta1 >=s and day_delta1 <= e:
            s = datetime.datetime.strptime(str(s)[:10], '%Y-%m-%d') + relativedelta(months=int(cate))
            e = datetime.datetime.strptime(str(e)[:10], '%Y-%m-%d') + relativedelta(months=int(cate))
            if e >= datetime.datetime.today() - datetime.timedelta(days=1):
                label=-1
#                return -1
            else:
                label=str(s)[:10] + ' - ' + str(e)[:10]
#                return str(s)[:10] + ' - ' + str(e)[:10]

    #logging.info('...END Ge_week_label')
    #clock()
    return label


def get_rate_result(df,cates,keys=['一期']):
    logging.info('...START Get_rate_result')
    clock()

    df = df.fillna(-1)
    df = df[(df['first_time']!=-1)]
    start_day = df['first_time'].min()
    end_day = df['first_time'].max()
    day_list = get_week_split(start_day=start_day,end_day=end_day,gap=7,cate=0)
    print 'day_list = ', day_list
    df['index_label']=df.apply(lambda x: get_week_label(x,day_list=day_list,day='first_time',cate=0),axis=1)

    data_final = []
    data = []
    cate_cal = []
    for cate in tqdm(cates):
        sub_data = []
        try:
            sub = df[(df[str(cate)+'_cate']==cate)]
        except KeyError,e:
            continue
        if len(sub)==0:
            continue
        day_df = df[(df['0_cate']==0)]
        start_day1 = day_df['0_auto_repay_time'].min()
        end_day1 = day_df['0_auto_repay_time'].max()
        day_list1 = get_week_split(start_day=start_day1,end_day=end_day1,gap=7,cate=cate)
        sub['repay_label']=sub.apply(lambda x: get_week_label(x,day_list=day_list1,day=str(cate)+'_auto_repay_time',cate=cate),axis=1)
        sub.fillna(-1)
        sub = sub[(sub['repay_label']!=-1)]
        for i in [1,2,3,7,15,30,0]:
            res = sub.groupby(['index_label','repay_label']).agg({str(cate)+'_overdue_'+str(i):['size','sum']})
            sub_data.append(res)
        sub_all=pd.concat(sub_data,axis=1)
        data.append(sub_all)
        cate_cal.append(cate)

    if len(data)==0:
        data_final = pd.DataFrame({'应还日期':[],'放款单数':[],'首逾单数':[],'未还单数':[],'首日':[],'2日':[],'3日':[],'7日':[],'15日':[],'30日':[],'截至现在':[]})
    else:
        for i in tqdm(range(0,len(data))):
            logging.info('...Cate ='+str(i))
            cate_df = data[i]
            cate = cate_cal[i]
            if cate_df.shape[0]==0:
                continue
            cate_cal_df = cal_rate(cate_df,cate)
            data_final.append(cate_cal_df)

    logging.info('...END Get_rate_result')
    clock()
    return data_final



def cal_rate(df,cate):
    logging.info('...START Calculate_rate...')
#    now = datetime.datetime.today() - datetime.timedelta(days=1)
    now = datetime.datetime.today()
    now = datetime.datetime.strptime(str(now)[:10], '%Y-%m-%d')
    day_dicts = ['2','3','7','15','30']
    result_cate = []
    keys_cate = []

    for (idx,row) in df.iterrows():
        result = {}
        indexs = []
        index_label,repay_label = idx
        indexs.append(index_label)
        result['loan_size']=int(row[str(cate)+'_overdue_1']['size'])
        result['delay1_size']=int(row[str(cate)+'_overdue_1']['sum'])
        result['norepay_size']=int(row[str(cate)+'_overdue_0']['sum'])
        result['repay_label']=repay_label
        for day_dict in day_dicts:
            max_day = repay_label.split(' - ')[1]
            max_day = datetime.datetime.strptime(str(max_day)[:10], '%Y-%m-%d')
            delta_day = float((now - max_day).days)
            if delta_day > float(day_dict):
                result[str(cate)+'_overdue_'+day_dict+'_rate'] = '%.2f%%' % (float(row[str(cate)+'_overdue_'+day_dict]['sum'])/float(row[str(cate)+'_overdue_'+day_dict]['size'])*100)
            else:
                result[str(cate)+'_overdue_'+day_dict+'_rate'] = ''
        result[str(cate)+'_overdue_1'+'_rate'] = '%.2f%%' % (float(row[str(cate)+'_overdue_1']['sum'])/float(row[str(cate)+'_overdue_1']['size'])*100)
        result[str(cate)+'_overdue_0'+'_rate'] = '%.2f%%' % (float(row[str(cate)+'_overdue_0']['sum'])/float(row[str(cate)+'_overdue_0']['size'])*100)
        result_df = pd.DataFrame(result,index=indexs)
        result_cate.append(result_df)
    final = pd.concat(result_cate,axis=0)
    final = final[['repay_label','loan_size','delay1_size','norepay_size',str(cate)+'_overdue_1'+'_rate',str(cate)+'_overdue_2'+'_rate',str(cate)+'_overdue_3'+'_rate',str(cate)+'_overdue_7'+'_rate',str(cate)+'_overdue_15'+'_rate',str(cate)+'_overdue_30'+'_rate',str(cate)+'_overdue_0'+'_rate']]
    final.columns=['应还日期','放款单数','首逾单数','未还单数','首日','2日','3日','7日','15日','30日','截至现在']
    final.sort_index(ascending=False,inplace=True)

    logging.info('...END Calculate_rate...')
    clock()
    return final


"""
if __name__ == '__main__':
    df = pd.DataFrame(pd.read_csv('/data/home/zhongwanxing/miracle/app/meitu_overdue.test.csv', sep=',', encoding='gbk'))
    df=df[['ca_id','log_type','user_id','cate','created_time','auto_repay_time','finish_repay_time','loan_special_time']]
    df = df[(df['auto_repay_time']!='2018-11-12 00:00:00')]
    cates = [0,1,2]
    keys = ['一期','二期','三期']
    df = get_first_auto_repay_time(df,cates)
    df = get_rate_result(df,cates,keys=keys)
"""