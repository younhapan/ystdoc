# -*- coding: utf-8 -*-
from __future__ import division
import sys
import os
import collections
import datetime
import pandas as pd
import pymysql
CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))


class Aggregator(object):
    def __init__(self):
        self.output = []
        self.index = ['id']
        self.retry_index = ['id','user_id','suggestion','error']
        self.data = dict()
        self.retry_data = dict()
        self.time = str(datetime.datetime.today())[0:10]
        self.location = {
            'cl_borrow': "%s/../data/mysql" % (CURRENT_DIR),
            'anubis_decision_info': "%s/../data/info" % (CURRENT_DIR),
            'feature_info': "%s/../data/feature" % (CURRENT_DIR)
        }

    # add index to self.index
    def append_index(self,index):
        for i in index:
            if i not in self.index:
                self.index.append(i)

    def prepare_cl_borrow(self):
        location = self.location['cl_borrow']+'/cl_borrow.'+ self.time
        a = open(location)
        b=[]
        for i in a.readlines():
            b.append(i[0:len(i)-1])
        index = b[0].split(',')
        self.append_index(index)
        for i in b[1:]:
            temp = i.split(',')
            if temp[0] not in self.data:
                self.data[temp[0]] = dict()
            for j in range(1,len(temp)):
                if index[j] not in self.data[temp[0]]:
                    self.data[temp[0]][index[j]] = temp[j]

    def prepare_anubis_decision_info(self):
        location = self.location['anubis_decision_info']+'/anubis_decision_info.'+ self.time
        a = open(location)
        b=[]
        for i in a.readlines():
            try:
                b.append(eval(i)['content'])
            except:
                print('anubis_decision_info file raise error')
        index = ['user_id','suggestion','created_time','type','baseline','score','te','epoch_score']
        self.append_index(index)
        for i in b:
            if i['suggestion'] == '2':
                if i['application_id'] not in self.retry_data:
                    self.retry_data[i['application_id']] = dict()
                    for j in self.retry_index[1:]:
                        self.retry_data[i['application_id']][j] = i.get(j,'nan')
            else:
                if i['application_id'] not in self.data:
                    self.data[i['application_id']] = dict()
                    for j in index:
                        self.data[i['application_id']][j] = i.get(j,'nan')
                else:
                    for j in index:
                        if j not in self.data[i['application_id']]:
                            self.data[i['application_id']][j] = i.get(j,'nan')

    def combine_retry_data(self,time=None,out_path=None):
        if time is not None:
            self.time = str(time)[0:4]+'-'+str(time)[4:6]+'-'+str(time)[6:8]
        if out_path is None: out_path = "%s/../data/retry_data" % (CURRENT_DIR)
        a=open(out_path+'/retry_data.'+self.time,'wb')
        out_str = ''
        for i in self.retry_index:
            out_str = out_str + str(i) + str('|')
        out_str = out_str[0:len(out_str)-1]
        a.write(bytes(out_str, encoding = "utf8")+b'\r\n')
        for i in self.retry_data:
            out_str = str(i) + str('|')
            for j in self.retry_index[1:]:
                if j in self.retry_data[i]:
                    out_str = out_str + str(self.retry_data[i][j]) + str('|')
                else:
                    out_str = out_str + 'nan' + str('|')
            out_str = out_str[0:len(out_str)-1]
            a.write(bytes(out_str, encoding = "utf8")+b'\r\n')
        a.close()

    # time默认值: 20200715
    def combine_basic_data(self,name=['cl_borrow','anubis_decision_info'],time=None,out_path=None):
        if time is not None:
            self.time = str(time)[0:4]+'-'+str(time)[4:6]+'-'+str(time)[6:8]
        if out_path is None: out_path = "%s/../data/basic_matrix" % (CURRENT_DIR)
        # load data
        self.prepare_cl_borrow()
        self.prepare_anubis_decision_info()
        #combine data
        a=open(out_path+'/basic_matrix.'+self.time,'wb')
        out_str = ''
        for i in self.index:
            out_str = out_str + str(i) + str('|')
        out_str = out_str[0:len(out_str)-1]
        a.write(bytes(out_str, encoding = "utf8")+b'\r\n')
        for i in self.data:
            out_str = str(i) + str('|')
            for j in self.index[1:]:
                if j in self.data[i]:
                    out_str = out_str + str(self.data[i][j]) + str('|')
                else:
                    out_str = out_str + 'nan' + str('|')
            out_str = out_str[0:len(out_str)-1]
            a.write(bytes(out_str, encoding = "utf8")+b'\r\n')
        a.close()


if __name__ == '__main__':
    a=Aggregator()
    try:
        time = str(sys.argv[1])
        a.combine_basic_data(time=time)
        a.combine_retry_data(time=time)
    except:
        a.combine_basic_data()
        a.combine_retry_data()
