#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import sys
import json
import glob
import time
from featurelib_utils.default_logger import default_logger

from featurelib_utils.ext_api import ext_api_ins

from sklearn.externals import joblib

module_dir = 'featureset'
module_suffix_extension = 'feature.py'

FeatureConf = os.path.dirname(__file__) + '/feature_conf/'

class FeatureProduction(object):

    def __init__(self, feature_conf_dict=None, logger_handler=None, ext_api=None):
        self.feature_conf_dict = feature_conf_dict
        self.ext_api = ext_api_ins
        self.feature_agent = {}
        self.logger = logger_handler if logger_handler else default_logger
        self._load_feature_desc()
        self.feature_objects = self._init_feature_objects() #按照配置文件初始化类

    def _init_feature_objects(self):
        feature_objects = {}
        for feature_name, fearure_ele in self.feature_conf_dict.iteritems():
            if 'features' in fearure_ele['out']:
                feature_objects[feature_name] = self.__all_feature_object[feature_name](self.ext_api, fearure_ele['out']['features'], logger_handler=self.logger)
            else:
                feature_objects[feature_name] = self.__all_feature_object[feature_name](self.ext_api, [], logger_handler=self.logger)
        return feature_objects

    def _init_feature_operator(self, feature_category):
        return None

    def _load_feature_desc(self):
        self.__all_feature_desc = {}
        self.__all_feature_object = {}
        current_dir = os.path.dirname(__file__)
        for pkg in glob.glob(current_dir+'/'+module_dir+'/*'+module_suffix_extension):
            base_name = os.path.basename(pkg).split('.')[0]
            pkg_name = current_dir.split('/')[-1]+'.'+module_dir+'.'+base_name
            base_names = base_name.split('_')
            module_name = ''
            for pn in base_names:
                pn = pn.capitalize()
                module_name += pn
            try:
                module = __import__(pkg_name, fromlist=[module_name])
            except Exception,e:
                print >>sys.stderr,'err loading--------------',pkg_name,module_name,e
                continue
            feature_class = getattr(module, module_name)
            self.__all_feature_object[module_name] = feature_class
            features_desc = feature_class.description_dict
            for f_id, f_desc in features_desc.iteritems():
                self.__all_feature_desc[f_id] = (f_desc, module_name)

    def get_features(self, feature_name=None, raw_data=None):
        time_start = time.time()
        if feature_name in self.feature_objects:
            feature_object = self.feature_objects[feature_name]
            ret_status,ret_data = feature_object.get_features(raw_data)
        else:
            ret_status,ret_data = False, 'no feature object'

        error_msg = None
        if not ret_status :
            error_msg="%s:%s" % (feature_name,ret_data)
        self.logger.info('info_type=%s\tis_online=%s\tFeatureNodeName=%s\tFeatureNodeStatus=%s\terror_msg=%s\ttime_cost=%.5f' % (
            'FeatureLibInfo',
            True,
            feature_name,
            ret_status,
            str(error_msg).replace('\t',' ').replace('=','#').replace(' ','_') if not ret_status else None,
            time.time() - time_start,
        ))
        return ret_status,ret_data

    def get_features_desc(self):
        return self.__all_feature_desc