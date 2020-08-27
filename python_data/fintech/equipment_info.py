#coding:utf8

import time, datetime
from feature_lib.featureset.base_feature import BaseFeature

class EquipmentInfoFeature(BaseFeature):
    def init(self):

        self.feature_map  = {
            100201 : self.get_equipment_info,
        }

    def get_core_data(self, raw_data = None):
        result          =   {
                                "info":raw_data["UserEquipmentInfo"]
                            }
        return result

    def is_rooted(self, info):
        if  not "rooted" in info:
            return "nan"
        if info["rooted"] == "false":
            return 0
        else:
            return 1

    def cal_gb(self, info, x):
        if not x in info:
            return -1
        elif info[x].split(' ')[1].upper() == 'GB':
            return float(info[x].split(' ')[0])
        elif info[x].split(' ')[1].upper() == 'MB':
            return float(info[x].split(' ')[0])/1024
        elif info[x].split(' ')[1].upper() == 'KB':
            return float(info[x].split(' ')[0])/1024/1024

    def cal_area(self, info):
        if (info.get('device_width', -1) == -1) or (info.get('device_height', -1) == -1):
            return -1
        else:
            return int(info['device_width'])*int(info['device_height'])

    def get_equipment_info(self):
        info        =   self.cdatamap["info"]
        print("*"*20)
        print(info)
        print("*"*20)
        result      =   {}
        result[1]   =   info.get("operating_system", "nan")
        result[2]   =   info.get("system_versions", "nan")
        result[3]   =   info.get("phone_brand", "nan")
        result[4]   =   info.get("phone_type", "nan")
        result[5]   =   info.get("version_name", "nan")
        result[6]   =   info.get("imei", "nan")
        result[7]   =   self.is_rooted(info)
        result[8]   =   int(info.get('device_width', -1))
        result[9]   =   int(info.get('device_height', -1))
        result[10]  =   info.get('default_language','nan')
        result[11]  =   info.get('sdk_version','nan')
        result[12]  =   self.cal_gb(info, 'ram_can_use')
        result[13]  =   self.cal_gb(info, 'ram_total')
        result[14]  =   self.cal_gb(info, 'cash_can_use')
        result[15]  =   self.cal_gb(info, 'cash_total')
        result[16]  =   self.cal_area(info)


        return  result







# encoding: utf-8
from __future__ import division
import sys
import os
import subprocess
import time
import datetime
import pandas as pd
import numpy as np
import math

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_PATH = "%s/../data" % (CURRENT_DIR)

today = datetime.datetime.today()
year = str(today.year)
month = '0' + str(today.month) if today.month < 10 else str(today.month)
day = '0' + str(today.day) if today.day < 10 else str(today.day)

try:
    subprocess.getoutput('rm '+DATA_PATH+'/anubis_decision_info.'+year+'-'+month+'-'+day)
except:
    print('no file1')
subprocess.getoutput('mkdir '+DATA_PATH+'/info')
subprocess.getoutput('aws s3 cp s3://ai.mumbai.newsdog.today/insta/online_log/anubis_decision/info/'+year+'-'+month+'-'+day+'/ '+DATA_PATH+'/info/ --recursive')
subprocess.getoutput('cat '+DATA_PATH+'/info/* > '+DATA_PATH+'/anubis_decision_info.'+year+'-'+month+'-'+day)
subprocess.getoutput('rm -rf '+DATA_PATH+'/info')




{"log_time":"2020-07-14 01:30:04.919","module":"decision_flow","process":"60","levelname":"INFO","host":"14179857b916","content":{"application_id": "183", "user_id": "644", "platform": "100", "data_type": "feature", "created_time": "2020-07-14 07:00:04",
"dump_data": "{\"1002010001\": \"android\", \"1002010002\": \"10\",\"1002010003\": \"Redmi\",\"1002010004\": \"Redmi Note 8 Pro\", \"1002010005\": \"1.0.3\", \"1002010006\": \"000000000000000\", \"1002010007\": 0,\"1002010008\": 1080, \"1002010009\": 2134,\"1002010010\": \"en\", \"1002010011\": \"10\", \"1002010012\": 4.21, \"1002010013\": 7.88, \"1002010014\": 31.35, \"1002010015\": 118.0, \"1002010016\": 2304720, \"2001010001\": 0, \"2001010003\": 0, \"2001010002\": -1, \"2001010004\": -1, \"2001010007\": -1, \"2001010076\": -1, \"2001010080\": -1, \"2001010006\": -1, \"2001010009\": -1, \"2001010005\": -1, \"2001010008\": -1, \"2001010010\": -1, \"2001010011\": -1, \"2001010019\": -1, \"2001010081\": -1, \"2001050001\": 2000.0, \"3001010001\": \"ASIYA MOHAMED MUSTUFA DADA\", \"3001010002\": \"ASIXX XXHAMED MUSTUFA DADA\", \"3001010003\": \"MOHAMED MUSTUFA EBRA\", \"3001010004\": \"ASIYA MOHAMED MUSTUFA DADA\", \"3001010005\": \"ASIF DHAMED MUSTUF ADA\", \"3001010006\": \"ASIYA MOHAMED MUSTUFA DADA\", \"3001010007\": \"ASIF DHAMED MUSTUF ADA\", \"3001010008\": 1, \"3001010009\": 2, \"3001010010\": \"bank_name\", \"3001010011\": 2, \"3001010012\": 0, \"3001010013\": 1, \"3002010001\": \"Asiya Mohamed Mustufa Dada\", \"3002010002\": -1, \"3002010003\": \"17/12/1989\", \"3002010004\": -1, \"3002010005\": \"FEMALE\", \"3002010006\": -1, \"3002010007\": \"997461017063\", \"3002010008\": -1, \"3002010012\": \"nan\", \"3002010013\": -1, \"3002010014\": \"400009\", \"3002010015\": -1, \"3002010017\": \"C/O Mohamed Mustufa Ebrahim Dada, Room no 21,1st Foor, Fatimabai Building, 13 Dharamshala Road, Dongri, S V P Road, Mumbai, Mumbai City, Maharashtra - 400009\", \"3002010018\": -1, \"3002010019\": \"nan\", \"3002010020\": -1, \"3002010021\": \"nan\", \"3002010022\": -1, \"3003010001\": \"MS00042618080735\", \"3003010002\": 664, \"3003010003\": \"MV1\", \"3004010001\": \"CBQPP5180B\", \"3004010002\": -1, \"3004010003\": \"SHABBIR AHMED SHAIKH\", \"3004010004\": -1, \"3004010005\": \"ASIF DHAMED MUSTUF ADA\", \"3004010006\": -1, \"3004010007\": \"17/12/1989\", \"3004010008\": -1, \"3004010009\": -1, \"3004010010\": -1, \"1001010001\": \"Asiya Mohamed Mustufa Dada\", \"1001010002\": 31, \"1001010003\": 1, \"1001010004\": \"9967696541\", \"1001010005\": \"17/12/1989\", \"1001010006\": \"nan\", \"1001010007\": \"married\", \"1001010011\": \"2020-07-14 06:02:46\", \"1001010012\": \"400009\", \"1001010013\": \"72.8336119,18.957474\", \"1001010014\": \"midfoods@yahoo.in\", \"1001010015\": \"yahoo\", \"1001010016\": \"mdmustufadada075@gmail.com\", \"1001010017\": \"gmail\", \"1001010018\": \"C/O Mohamed Mustufa Ebrahim Dada, Room no 21,1st Foor, Fatimabai Building, 13 Dharamshala Road, Dongri, S V P Road, Mumbai, Mumbai City, Maharashtra - 400009\", \"1001010019\": \"maharashtra\", \"1001010020\": \"mah\", \"1001010021\": -1, \"1001010022\": -1, \"1001010023\": \"279/281, Sardar Vallabhbhai Patel Rd, Mandvi, Mumbai, Maharashtra 400003, India\", \"1001010024\": \"maharashtra\", \"1001010025\": \"mah\", \"1001010026\": -1, \"1001010027\": -1,\"1001010101\": 30,\"1001010102\": \"1989\", \"1001010103\": \"12\", \"1001010104\": \"17\", \"1001010105\": \"maharashtra\", \"1001010106\": \"mumbai\", \"1001010107\": \"nan\", \"1001010108\": 34, \"1001010109\": 0, \"1001010110\": 0, \"1001030001\": \"Manager\", \"1001030011\": \"MD TUTORIALS\", \"1001030012\": \"nan\", \"1001030013\": \"nan\", \"1001030014\": \"nan\", \"1001030015\": \"nan\", \"1001030021\": \"Bank Transfer\", \"1001030022\": \"25001-35000\", \"1001030024\": \"nan\", \"1001030101\": \"nan\", \"1001030102\": \"nan\", \"1001030103\": -1, \"1001030104\": -1, \"1001040001\": \"Maharashtra,Mumbai\", \"1001040002\": \"maharashtra\", \"1001040003\": \"mah\", \"1001040004\": -1, \"1001040005\": -1, \"1001040013\": \"nan\", \"1001040014\": \"400009\", \"1001040015\": \"maharashtra\", \"1001040016\": \"mumbai\",\"1001040017\": \"nan\",\"1001040018\": 34, \"1001040019\": 0, \"1001040020\": 0, \"1001050001\": \"20\", \"1001050002\": \"10\", \"1003010001\": \"PAYTM PAYMENTS BANK LTD\", \"1003010002\": \"MOHAMED MUSTUFA EBRA\", \"1003010003\": \"PYTM0123456\", \"4001010001\": 1, \"4001010002\": 4645,\"4001010003\": 2892,\"4001010004\": 3,\"4001010005\": 0.001037344398340249, \"4001010021\": 0.15525587828492393, \"4001010022\": 8, \"4001010023\": 0.0027662517289073307, \"5001010001\": 4002,\"5001010002\": 0,\"5001010003\": 240, \"5001010004\": 238, \"5001010005\": 1598,\"5001010006\": 1554,\"9991001000\": [\"UserContactFeature\", \"EquipmentInfoFeature\", \"UserHistFeature\", \"ThirdPartyFeature\", \"UserBaseInfoFeature\", \"UserMessageFeature\"]}"}}





user_id = 644

def make_model_decision(self,fmap):
    score = 1000

    brand = fmap[1002010003].lower() #Redmi
    width = int(fmap[1002010008]) #1080
    height = int(fmap[1002010009]) #2134
    age = int(fmap[1001010101]) #30
    state_level = int(fmap[1001040017]) if fmap[1001040017] != 'nan' else 'nan' #nan
    message_num = int(fmap[5001010002]) #0
    overdue_message = int(fmap[5001010006]) #1554
    contact_num = int(fmap[4001010003]) #2892
    family_num = int(fmap[4001010004]) #3

    if brand not in ['huawei','samsung']: score-=20 #-20
    if width < 800 and height < 2100: score-=50
    if age < 22 or age > 35: score-=50
    if state_level != 'nan': score -= 20*(state_level-1)
    if message_num < 100: score-=20 # -20
    if overdue_message > 5: score-=100 # -100
    score -= 5*overdue_message # -5*1554
    if contact_num < 50: score-=20
    if family_num == 0: score-=20

    return score



"baseline": "['BaselineRedzone']", "score": "-6910", "epoch_score": "664",
相关的值不存在
user_id = 638, 600
suggestion = 2



{"log_time":"2020-07-14 00:11:55.692","module":"decision_flow","process":"60","levelname":"INFO","host":"14179857b916",
"content":{"application_id": "158", "user_id": "600", "platform": "100", "data_type": "feature", "created_time": "2020-07-13 18:40:18",
"dump_data": "{"1002010001": "android", "1002010002": "9", "1002010003": "OPPO", "1002010004": "CPH1881", "1002010005": "1.0.3", "1002010006": "865468045013896", "1002010007": 0, "1002010008": 1080, "1002010009": 2116, "1002010010": "en", "1002010011": "9", "1002010012": 1.32, "1002010013": 3.88, "1002010014": 33.23, "1002010015": 53.8, "1002010016": 2285280, "2001010001": 0, "2001010003": 0, "2001010002": -1, "2001010004": -1, "2001010007": -1, "2001010076": -1, "2001010080": -1, "2001010006": -1, "2001010009": -1, "2001010005": -1, "2001010008": -1, "2001010010": -1, "2001010011": -1, "2001010019": -1, "2001010081": -1, "2001050001": 2000.0, "3001010001": "SIVARAMAN M", "3001010002": "nan", "3001010003": "MSIVARAMAN", "3001010004": "SIVARAMAN M", "3001010005": "SIVARAMAN M", "3001010006": "SIVARAMAN M", "3001010007": "SIVARAMAN M", "3001010008": 1, "3001010009": 2, "3001010010": "bank_name", "3001010011": 0, "3001010012": 1, "3001010013": 0, "3002010001": "Sivaraman M", "3002010002": -1, "3002010003": "07/02/1986", "3002010004": -1, "3002010005": "MALE", "3002010006": -1, "3002010007": "582045563573", "3002010008": -1, "3002010012": "nan", "3002010013": -1, "3002010014": "626108", "3002010015": -1, "3002010017": "S/O Munkara Nayakkar, 38/2, KAMMAPATTI, KALANGAPERI, Arasiyarpatti, Virudhunagar, Tamil Nadu - 626108", "3002010018": -1, "3002010019": "nan", "3002010020": -1, "3002010021": "nan", "3002010022": -1, "3003010001": "MS40188914449439", "3003010002": 543, "3003010003": "MV1", "3004010001": "JHCPS6716P", "3004010002": -1, "3004010003": "MUNKARA NAYAKKAR", "3004010004": -1, "3004010005": "SIVARAMAN M", "3004010006": -1, "3004010007": "37/02/1986", "3004010008": -1, "3004010009": -1, "3004010010": -1, "4001010001": 1, "4001010002": 596, "4001010003": 540, "4001010004": 0, "4001010005": 0.0, "4001010021": 0.8240740740740741, "4001010022": 376, "4001010023": 0.6962962962962963, "5001010001": 2226, "5001010002": 0, "5001010003": 142, "5001010004": 123, "5001010005": 1372, "5001010006": 1116,
"9991001000": ["UserContactFeature", "EquipmentInfoFeature", "UserHistFeature", "ThirdPartyFeature", "UserMessageFeature"]}"}}





