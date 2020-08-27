# https://github.com/orgs/tmrwh/teams/fintech/repositories
# anubis_application_service anubis_decision_service anubis_result_service anubis_status_service: 这四个是决策引擎和业务系统交互附属包
# ice_core: 决策引擎
# feature_lib: 特征计算逻辑库
# feature_bank: 离线刷特征工具
# tethys: 数据获取特征框架
# themis: 特征计算调度框架
# lambda_core: 对外请求数据接口整合
# amon: 离线模型训练工具
# maat: 在线模型加载工具
# daixiaomi_common: 公共服务，打印日志，消费队列，异常获取
# miracle: 监控，数据分析工具

# 主要看ice_core feature_lib amon maat miracle

#!/usr/bin/env python
# encoding: utf-8
import sys
import os
import traceback
import socket
import time
import urllib
import json
import datetime

import re

def get_good_contacts(contacts):
    """ 通讯录数据优化，剔除杂质：
        1、去掉数字姓名
        2、去掉短号码
        3、去掉有8个以上号码的联系人
    """
    num_map = {}
    good_contacts = []
    for c in contacts:
        if c.name.isdigit():
            continue
        if len(c.phone) <= 6:
            continue
        if c.name in num_map:
            num_map[c.name].append(c)
        else:
            num_map[c.name] = [c]

    for k,v in num_map.items():
        if len(v) >= 8:
            continue
        else:
            good_contacts.append(v)
    good_list = []
    for i in good_contacts:
        for j in i:
            good_list.append(j)
    return good_list

def pos_in_name(name):
    """判断名name内是否出现了POS或POSS（但不包括possibility等单词）"""
    name = name.replace(' ','')
    name = name.decode('utf8')
    # 匹配所有中文字符
    pat = u"[\u4e00-\u9fa5]{1}"
    # 把所有中文换成空格
    tmp = re.sub(pat,' ',name)
    # 删除开头结尾空格，并将多个连续空格变为一个
    words = ' '.join(tmp.split())
    if words == '':
        return False
    words = words.split(' ')
    for w in words:
        if w.upper() == 'POS' or w.upper()=='POSS' or w.upper()=='POSJI':
            return True
    return False

def has_number(name):
    """判断名name内是否出现了数字（包括中文的数字）"""
    if bool(re.search(r'\d',name)):
        return True
    num_str = ['一','二','三','四','五','六','七','八','九','十']
    for s in num_str:
        if s in name:
            return True
    return False

def name_is_tonghang(name0, suspicious_name_list):
    """判断称呼name0是否命中同行名"""
    # 转码
    try:
        name = name0.encode('utf8')
    except:
        name = name0
    # 处理同时带数字和'借'、'贷'可疑词的情况
    if has_number(name):
        for sus_name in suspicious_name_list['tonghang']['name_with_number']:
            if sus_name in name:
                return True, '数字+'+sus_name
    # 处理pos或pos机的情况
    if pos_in_name(name):
        return True, 'POS或POSS'
    for sus in suspicious_name_list['tonghang']['name']:
        if isinstance(sus,str):
        # 处理可疑词仅是一个单词的情况
            if sus in name:
                return True, sus
        elif isinstance(sus,list):
        # 处理可疑词是多个词的情况
            flag = 0
            for s in sus:
                if s in name:
                    flag += 1
            if flag == len(sus):
               return True, '+'.join(sus)
    return False, '称呼'+name+'未命中同行名'

def name_is_suspicious(name0, suspicious_name_list):
    """判断称呼name0是否是可疑的"""
    # 转码
    try:
        name = name0.encode('utf8')
    except:
        name = name0
    levels = [level for level in suspicious_name_list.keys() if isinstance(level,int)]
    levels.sort()
    for level in levels:
        for sus in suspicious_name_list[level]['name']:
            if isinstance(sus,str):
            # 处理可疑词仅是一个单词的情况
                if sus in name:
                    return True, level, sus
            elif isinstance(sus,list):
            # 处理可疑词是多个词的情况
                flag = 0
                for s in sus:
                    if s in name:
                        flag += 1
                if flag == len(sus):
                    return True, level, '+'.join(sus)
        # 带数字的情况
        if has_number(name):
            for sus_name in suspicious_name_list[level]['name_with_number']:
                if sus_name in name:
                    return True, level, '数字+'+sus_name
    return False, -1, '称呼'+name+'未命中同行名'