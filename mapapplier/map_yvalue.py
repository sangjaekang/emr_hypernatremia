# -*- coding: utf-8 -*-
import sys, os, re

os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)
from mapapplier.config import *
from mapapplier.map_common import convert_month, check_directory, save_to_hdf5


def run():
    global PER_LAB_DIR, PREP_OUTPUT_DIR, PATIENT_COUNT, USE_LAB_COL_NAME, SAMPLE_PATIENT_PATH, DEBUG_PRINT

    # syntax checking existence for directory
    PER_LAB_DIR = check_directory(PER_LAB_DIR)
    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)
    output_path = PREP_OUTPUT_DIR + SAMPLE_PATIENT_PATH

    result_df = pd.DataFrame(index=range(1,PATIENT_COUNT))

    re_per_lab = re.compile("^labtest_.*\.csv")     
    for file_name in os.listdir(PER_LAB_DIR):
        if re_per_lab.match(file_name):
            
            per_lab_name = file_name.replace('labtest_','').replace('.csv','')
            per_lab_path = PER_LAB_DIR + file_name
            per_lab_df = pd.read_csv(per_lab_path, delimiter = DELIM, usecols=USE_LAB_COL_NAME)
            result_df[per_lab_name] = per_lab_df.drop_duplicates(['no','date']).groupby(['no']).count().date
            
            if DEBUG_PRINT:
                print("{} is clear".format(file_name))

    result_df = result_df.count(1).to_frame('count')
    result_df.index.name = 'no'

    result_df.to_hdf5(output_path,"metadata/patient_count",format='table',data_columns=True,mode='a')
    del result_df

