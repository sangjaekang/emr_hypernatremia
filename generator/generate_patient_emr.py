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


def mean_with_nan(x,y):
    len_x = len(x.values)
    result_series = pd.Series(index=x.index)
    
    for i in range(len_x):
        if np.isnan(x.values[i]):
            if np.isnan(y.values[i]):
                result_series.iloc[i] = np.nan
            else:
                result_series.iloc[i] = y.values[i]
        else:
            if np.isnan(y.values[i]):
                result_series.iloc[i] = x.values[i]
            else:
                result_series.iloc[i] = np.mean((x.values[i],y.values[i]))
    return result_series


def change_label(hyper_na,hypo_na):    
    def _change_label(x):
        if np.isnan(x):
            return np.nan
        else :
            if x > hyper_na:
                return 2
            elif x < hypo_na:
                return 1
            else : 
                return 0
    return _change_label


def generate_emr_with_na_result(no,emr_types={'demo','diag','lab','pres'}):
    emr_df = generate_emr(no,emr_types)

    labtest_df = pd.HDFStore(,mode='r')
    mapping_df = labtest_df.select('metadata/mapping_table')
    labtest_df.close()
    _,na_avg,na_min,na_max = mapping_df[mapping_df.labtest == 'L3041'].values[0]
    _,na_e_avg,na_e_min,na_e_max = mapping_df[mapping_df.labtest == 'L8041'].values[0]

    hyper_na = (145-na_min)/(na_max-na_min);    hypo_na  = (135-na_min)/(na_max-na_min)
    hyper_e_na = (145-na_e_min)/(na_e_max-na_e_min);    hypo_e_na = (135-na_e_min)/(na_e_max-na_e_min)
    
    na_result = a.loc['L3041'].map(change_label(hyper_na,hypo_na))
    na_e_result = a.loc['L8041'].map(change_label(hyper_e_na,hypo_e_na))
    
    t_result = mean_with_nan(na_e_result,na_result)
    emr_df['result'] = t_result

    return emr_df
