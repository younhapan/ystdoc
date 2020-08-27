#!/usr/bin/env python
# encoding: utf-8

import time
import datetime
import hashlib
import traceback
import math
import numpy

class CommonUtils(object) :
    def __init__(self) :
        pass

    def str_to_datetime(self,str_time) :
        """
        将YY-mm-dd HH:MM:SS 类型字符串转化成datetime
        """
        return datetime.datetime.strptime(str(str_time),'%Y-%m-%d %H:%M:%S')

    def str_to_time(self,str_time):
        """
        将YY-mm-dd HH:MM:SS 类型字符串转化成unix 时间戳
        """
        return time.mktime(time.strptime(str(str_time)[:19],'%Y-%m-%d %H:%M:%S'))

    def list_excavate(self,_list) :
        return {
            'len':self.list_len(_list),
            'sum':self.list_sum(_list),
            'ave':self.list_ave(_list),
            'max':self.list_max(_list),
            'min':self.list_min(_list),
            'median':self.list_median(_list),
            'variance':self.list_variance(_list),
            'standard':self.list_standard(_list),
        }

    def matchming_number(self, matchming_number_list, ii) :
        """
        #将matchming_number_list的数据拟合成一次函数  获取x坐标为ii的数据
        #matchming_number_list 是个元组的list (x,y)
        """
        matchming_number_list = sorted(matchming_number_list,key=lambda matchming_number:matchming_number[0])
        X = []
        Y = []
        for (a,b) in matchming_number_list :
            X.append(a)
            Y.append(b)
        zx = numpy.polyfit(X, Y, 1)
        func = numpy.poly1d(zx)
        return func(ii)

    def list_len(self,_list) :
        """
        list 长度
        """
        if type(_list) != list or len(_list) == 0:
            return 'nan'
        return int(len(_list))

    def list_sum(self,_list) :
        """
        list 求和
        """
        try :
            return sum(_list) if len(_list) != 0 else 'nan'
        except :
            return 'err'

    def list_ave(self,_list) :
        """
        list 平均值
        """
        try :
            return sum(_list) * 1.0 / len(_list) if len(_list) != 0 else 'nan'
        except :
            return 'err'

    def list_max(self,_list) :
        """
        list 最大值
        """
        try :
            return max(_list) if len(_list) != 0 else 'nan'
        except :
            return 'err'

    def list_min(self,_list) :
        """
        list 最小值
        """
        try :
            return min(_list) if len(_list) != 0 else 'nan'
        except :
            return 'err'

    def list_median(self,_list) :
        """
        list 中位数
        """
        try :
            if len(_list) == 0 : return 'nan'
            _list = sorted(_list)
            if len(_list) % 2 == 1 :
                return _list[len(_list)//2]
            else :
                return ( _list[len(_list)//2-1] + _list[len(_list)//2] ) * 1.0 / 2
        except :
            return 'err'

    def list_variance(self,_list) :
        """
        list 方差
        """
        try :
            if len(_list) == 0 : return 'nan'
            ex=float(sum(_list))/len(_list)
            s=0
            for i in _list:
                s+=(i-ex)**2
            return float(s)/len(_list)
        except :
            return 'err'

    def list_standard(self,_list) :
        """
        list 标准差
        """
        try :
            if len(_list) == 0 : return 'nan'
            ex=float(sum(_list))/len(_list);
            s=0;
            for i in _list:
                s+=(i-ex)**2;
            return math.sqrt(float(s)/len(_list));
        except :
            return 'err'


    def md5(self,_str):
        """
        将str转化为md5值
        """
        m = hashlib.md5()
        m.update(_str)
        return m.hexdigest()

    def sigmoid(self,w1,w2) :
        """
        sigmoid函数
        """
        def get_prob_by_weight(w1,w2) :
            new_weight = w1 + w2
            new_prob = 1.0 / (1 + math.e**(-1.0 * new_weight))
            return new_prob
        return get_prob_by_weight(float(w1),float(w2))

    def overdue_days(self,auto_repay_time,finish_repay_time):
        """
        通过auto_repay_time 和 finish_repay_time 计算 逾期天数   copy hydra
        0:未逾期
        -2:未放款
        -1:未到期
        """
        if auto_repay_time:
            if auto_repay_time and auto_repay_time > datetime.datetime.now()+datetime.timedelta(hours=-8):
                return -2
            dt = finish_repay_time if finish_repay_time else datetime.datetime.now()+datetime.timedelta(hours=-8)
            deti = dt - auto_repay_time
            days = deti.days
            seconds = deti.seconds
            if days < 0:
                num = 0
            elif days == 0:
                if seconds < 12 * 60 * 60:
                    num = 0
                else:
                    num = 1
            elif days > 0:
                num = days + 1
        else:
            num = -1
        return num