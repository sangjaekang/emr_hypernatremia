# -*- coding: utf-8 -*-
import sys, os, re

os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)
from mapapplier.config import *
from mapapplier.map_common import convert_month, check_directory, save_to_hdf5

def convert_to_numeric(x):
    str_x = str(x)
    return float(str_x.replace(">","").replace("<",""))


def get_na_label_df():
    global PER_LAB_DIR, PREP_OUTPUT_DIR, PATIENT_COUNT, USE_LAB_COL_NAME, SAMPLE_PATIENT_PATH, DEBUG_PRINT
    # syntax checking existence for directory
    PER_LAB_DIR = check_directory(PER_LAB_DIR)
    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)
    output_path = PREP_OUTPUT_DIR + SAMPLE_PATIENT_PATH

    result_df = pd.DataFrame(index=range(1,PATIENT_COUNT))
    na_df = pd.read_csv(PER_LAB_DIR+'labtest_L3041.csv')
    na_e_df = pd.read_csv(PER_LAB_DIR+'labtest_L8041.csv')

    na_df.date = na_df.date.map(convert_month)
    na_e_df.date = na_e_df.date.map(convert_month)

    na_df.result = na_df.result.map(convert_to_numeric)
    na_e_df.result = na_e_df.result.map(convert_to_numeric)

    na_df.loc[na_df.result<135,'result'] = 1
    na_df.loc[(na_df.result>=135)&(na_df.result<=145),'result'] = 0
    na_df.loc[na_df.result>145,'result'] = 2

    na_e_df.loc[na_e_df.result<135,'result'] = 1
    na_e_df.loc[(na_e_df.result>=135)&(na_e_df.result<=145),'result'] = 0
    na_e_df.loc[na_e_df.result>145,'result'] = 2

    total_df = pd.concat([na_df,na_e_df])
    total_df = total_df.groupby(['no','date','result']).size().unstack(fill_value=0)
    total_df = (2*(total_df[2.0]>0))+(1*(total_df[1.0]>0))
    total_df = total_df.reset_index()
    total_df.columns = ['no','date','label']

    total_df.to_hdf(output_path,"data/na_label",format='table',data_columns=True,mode='a')


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

    result_df.to_hdf(output_path,"metadata/patient_count",format='table',data_columns=True,mode='a')
    del result_df
