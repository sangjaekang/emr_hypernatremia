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
from generator.construct_common import check_directory, save_to_hdf5, get_timeseries_column, get_time_interval

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



def get_patient_timeseries_label(no,label_name):
    global  PREP_OUTPUT_DIR,  SAMPLE_PATIENT_PATH
    # syntax checking existence for directory
    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)
    output_path = PREP_OUTPUT_DIR + SAMPLE_PATIENT_PATH
    x = pd.HDFStore(output_path,mode='r').select('data/{}'.format(label_name))

    result_series = pd.Series(index=get_timeseries_column())
    
    for _,x_date,x_label in x[x.no==no].values:
        result_series[x_date]=x_label
    
    return result_series


def check_label(x):
    both_exists = x.isin([3]).sum()
    hyper_exists = x.isin([2]).sum()
    hypo_exists = x.isin([1]).sum()
    normal_exists = x.isin([0]).sum()
    if both_exists:
        return 3
    if hyper_exists:
        return 2
    if hypo_exists:
        return 1
    if normal_exists:
        return 0
    else:
        return np.nan


def save_patient_input(no_range,label_name,input_dir=None,time_length=6,gap_length=1,target_length=3,offset_detect_counts=100):
    global INPUT_DIR, DEBUG_PRINT
    if input_dir is None:
        input_dir = INPUT_DIR

    for no in no_range:
        emr_df = generate_emr(no,{'lab'})
        label_series = get_patient_timeseries_label(no,label_name)

        colist = list(emr_df.columns)

        for i in range(0,len(colist) - (time_length+gap_length+target_length)):
            window = emr_df.loc[:,colist[i]:colist[time_length-1+i]]
            if window.count().sum() > offset_detect_counts:
                target_stime = colist[time_length-1+gap_length+i]
                label_value = check_label(label_series.loc[target_stime:target_stime+target_length-1])
                
                if np.isnan(label_value): continue

                o_path = input_dir +'{}/'.format(label_value)
                if not os.path.isdir(o_path):
                    os.makedirs(o_path)
                    
                file_path = o_path + "{}_{}.npy".format(no,i)
                np.save(file_path,window.as_matrix())
                if DEBUG_PRINT:
                    print("{} is saved!".format(file_path))