#-*- coding: utf-8 -*-
import sys, os, re

os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)
from generator.config import *
from generator.construct_common import check_directory ,get_time_interval
from generator.generate_patient_emr import generate_emr, get_patient_timeseries_label, save_patient_input

from imputator.impute_mean import *
import h5py
import time
import multiprocessing


if __name__ == "__main__":
    global PREP_OUTPUT_DIR, SAMPLE_PATIENT_PATH
    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)
    output_path = PREP_OUTPUT_DIR + SAMPLE_PATIENT_PATH
    
    sample_store = pd.HDFStore(output_path,mode='r')
    a = sample_store.select('data/na_label')
    sample_store.close()

    target_0_1 = a[a.label==0].no.unique()[:10000]
    target_0_2 = a[a.label==0].no.unique()[10000:20000]

    target_1_1 = a[a.label==1].no.unique()[:10000]
    target_1_2 = a[a.label==1].no.unique()[10000:20000]

    target_2_1 = a[a.label==2].no.unique()[:10000]
    target_2_2 = a[a.label==2].no.unique()[10000:20000]

    target_3_1 = a[a.label==3].no.unique()[:10000]

    print("Creating pool with 7 workers")
    pool = multiprocessing.Pool(processes=7)
    print(pool._pool)
    print("Invoking apply(save_patient_input, no_range)")

    pool.apply(save_patient_input,[target_0_1])
    pool.apply(save_patient_input,[target_0_2])
    
    pool.apply(save_patient_input,[target_1_1])
    pool.apply(save_patient_input,[target_1_2])
    
    pool.apply(save_patient_input,[target_2_1])
    pool.apply(save_patient_input,[target_2_2])

    pool.apply(save_patient_input,[target_3_1])

    pool.close()
    pool.join()
    
    print("Finished")