# -*- coding: utf-8 -*-
import sys, os, re

os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)
from generator.config import *

from generator.construct_demographic import get_demo_df
from generator.construct_diagnosis import get_diagnosis_df
from generator.construct_labtest import get_labtest_df
from generator.construct_prescribe import get_prescribe_df


def generate_emr(no,emr_types={'demo','diag','lab','pres'}):
    '''
    emr_data를　구성할　때，　어떤　dataset의　합으로　구성할　것인지　선택
    emr_type은　set형식으로，　원하는　dataset의　이름을　넣으면됨

    type :
        'demo' : 환자의 신상 정보
        'diag' : 환자의 질병 진단 정보
        'lab'  : 환자의 labtest 정보
        'pres' : 환자의 약물 처방 정보
    '''
    emr_list = []
    if 'demo' in emr_types:
        emr_list.append(get_demo_df(no))
    if 'diag' in emr_types:
        emr_list.append(get_diagnosis_df(no))
    if 'lab' in emr_types:
        emr_list.append(get_labtest_df(no))
    if 'pres' in emr_types:
        emr_list.append(get_prescribe_df(no))
    
    emr_df = pd.concat(emr_list)
    del emr_list
    return emr_df