# -*- coding: utf-8 -*-
import sys, os, re

os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)
from mapapplier.config import *
from mapapplier.map_common import convert_month, check_directory

def normalize_number(mean_x,min_x,max_x):
    '''
    dataframe 내 이상값을 전처리하는 함수.
    dataframe.map 을 이용할 것이므로, 함수 in 함수 구조 사용
    '''
    def _normalize_number(x):
        str_x = str(x).strip()

        re_num = re.compile('^[+-]?[\d]+[.]?[\d]*$')
        re_lower = re.compile('^<[\d\s]*[.]{0,1}[\d\s]*$')
        re_upper = re.compile('^>[\d\s]*[.]{0,1}[\d\s]*$')
        re_star = re.compile('^[\s]*[*][\s]*$')
        if re_num.match(str_x):
            # 숫자형태일경우
            float_x = np.float(str_x)
            if float_x > max_x:
                return 1
            elif float_x < min_x:
                return 0
            else:
                return (np.float(str_x) - min_x)/(max_x-min_x)
        else:
            if re_lower.match(str_x):
                return np.float(0)
            elif re_upper.match(str_x):
                return  np.float(1)
            elif re_star.match(str_x):
                return np.float( (mean_x-min_x)/(max_x-min_x) )
            else:
                return np.nan

    return _normalize_number


def get_labtest_value(df,labtest_name):
    global MAP_LAB_COL_NAME
    _temp = df[df.labtest.isin([labtest_name])]
    return _temp[MAP_LAB_COL_NAME[1]].values[0], _temp[MAP_LAB_COL_NAME[2]].values[0], _temp[MAP_LAB_COL_NAME[3]].values[0]


def get_labtest_map():
    global MAPPING_DIR, LAB_MAPPING_PATH, DELIM

    MAPPING_DIR = check_directory(MAPPING_DIR)
    lab_mapping_path = MAPPING_DIR + LAB_MAPPING_PATH

    if not os.path.isfile(lab_mapping_path):
        raise ValueError("There is no labtest_OUTPUT file!")

    labtest_mapping_df = pd.read_csv(lab_mapping_path,delimiter=DELIM)
    return labtest_mapping_df


def run():
    global PER_LAB_DIR, PREP_OUTPUT_DIR, LAB_COL_NAME, USE_LAB_COL_NAME, LABTEST_OUTPUT_PATH, DEBUG_PRINT

    # syntax checking existence for directory
    PER_LAB_DIR = check_directory(PER_LAB_DIR)
    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)
    output_path = PREP_OUTPUT_DIR + LABTEST_OUTPUT_PATH

    # if the previous output file exists, remove it
    if os.path.isfile(output_path):    
        os.remove(output_path)
    # get mapping dataframe and save to hdf5 file 
    labtest_mapping_df = get_labtest_map()
    labtest_mapping_df = labtest_mapping_df.apply(pd.to_numeric,errors='ignore')
    labtest_mapping_df.to_hdf(output_path, "metadata/mapping_table", format='table',date_columns=True, mode='a')

    re_per_lab = re.compile("^labtest_.*\.csv")     
    for file in os.listdir(PER_LAB_DIR):
        if re_per_lab.match(file):
            per_lab_name = file.replace('labtest_','').replace('.csv','')
            per_lab_path = PER_LAB_DIR + file
            per_lab_df = pd.read_csv(per_lab_path, delimiter = DELIM,
                                    usecols=USE_LAB_COL_NAME)
            # 1. 값　가져오기
            r_avg, r_min, r_max = get_labtest_value(labtest_mapping_df, per_lab_name)
            per_lab_df.result =  per_lab_df.result.map(normalize_number(r_avg,r_min,r_max))
            per_lab_df.date = per_lab_df.date.map(convert_month) 
            # file type change
            save_name = 'data/' + per_lab_name
            per_lab_df = per_lab_df.apply(pd.to_numeric,errors='ignore')            
            per_lab_df.to_hdf(output_path, save_name, format='table', data_columns=True, mode='a')

            if DEBUG_PRINT:
                print("{} dataframe enters hdf5 file".format(per_lab_name))


if __name__=='__main__':
    run()    