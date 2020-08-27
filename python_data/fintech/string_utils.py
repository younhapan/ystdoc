#!/usr/bin/env python
# encoding: utf-8
import datetime

def is_chinese(uchar):
    if uchar >= u'\u4e00' and uchar<=u'\u9fa5':
        return True
    else:
        return False

def is_alphabet(uchar):

    """判断一个unicode是否是英文字母"""

    if (uchar >= u'\u0041' and uchar<=u'\u005a') or (uchar >= u'\u0061' and uchar<=u'\u007a'):
        return True
    else:
        return False

def is_number(uchar):
    if uchar >= u'\u0030' and uchar<=u'\u0039':
        return True
    else:
        return False

def is_other(uchar):
    if not (is_chinese(uchar) or is_number(uchar) or is_alphabet(uchar)):
        return True
    else:
        return False

def get_only_alphabet_and_chinese(name):
    a = ""
    if isinstance(name,str):
        name = name.decode("utf-8")
    for u in name:
        if is_chinese(u) or is_alphabet(u):
            a += u
    return a.encode("utf-8")

def get_only_alphabet(name):
    a = ""
    tt = name
    if not isinstance(name,unicode):
        tt = name.decode("utf-8")
    for u in tt:
        if is_alphabet(u):
            a += u
    return a.encode("utf-8")

def get_only_chinese(name):
    a = ""
    tt = name
    if not isinstance(name,unicode):
        tt = name.decode("utf-8")
    for u in tt:
        if is_chinese(u):
            a += u
    return a.encode("utf-8")

def get_only_num(name):
    a = ""
    tt = name
    if not isinstance(name,unicode):
        tt = name.decode("utf-8")
    for u in tt:
        if is_number(u):
            a += u
    return a.encode("utf-8")

def have_same_word(name,name2):
    if isinstance(name,str) and isinstance(name2,str):
        n1 = name.decode("utf-8")
        n2 = name2.decode('utf-8')
        for u in n1:
            for l in n2:
                if u ==l:
                    return True
    return False

def user_id_ok(uid):
    if uid != None and uid != "None" and uid != "":
        return True
    return False

def get_only_location_name(name):
    name = name.replace('省', '')
    name = name.replace('市', '')
    name = name.replace('地区', '')
    name = name.replace('自治区', '')
    return name

def get_only_head2word(name):
    a = ""
    if isinstance(name, str):
        name = name.decode("utf-8")
    num = 0
    for u in name:
        if is_chinese(u):
            a += u
            num += 1
        if num >= 2:
            break
    return a.encode("utf-8")