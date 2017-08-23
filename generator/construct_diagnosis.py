# -*- coding :utf-8 -*-
import sys, os, re

os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)
from generator.config import *
from generator.construct_common import check_directory, save_to_hdf5, get_timeseries_column, get_time_interval

# output path setting
global PREP_OUTPUT_DIR, DIAGNOSIS_OUTPUT_PATH
PREP_OUTPUT_DIR= check_directory(PREP_OUTPUT_DIR)    
diagnosis_output_path = PREP_OUTPUT_DIR + DIAGNOSIS_OUTPUT_PATH

def set_diagnosis_row():
    '''
    진단코드를row_index_name으로나열
    OFFSET_DIAGNOSIS_COUNTS 기준에　따라서，drop 할　row을　결정
    drop하고　남은　row를　metadata/usecol에　저장
    '''
    global OFFSET_DIAGNOSIS_COUNTS, diagnosis_output_path

    store_diag = pd.HDFStore(diagnosis_output_path,mode='r')
    if not '/data' in store_diag.keys():
        raise ValueError("There is no data in diagnosis data")

    diag_df = store_diag.select('data')
    value_counts_diag = diag_df.KCD_code.value_counts()
    total_diag = diag_df.KCD_code.sum()
    min_value_counts = int(total_diag * OFFSET_DIAGNOSIS_COUNTS)

    use_index_df = value_counts_diag[value_counts_diag>min_value_counts].sort_index().reset_index()
    use_index_df.columns = ['col','value_counts']
    store_diag.close()
    use_index_df.to_hdf(diagnosis_output_path,'metadata/usecol',format='table',data_columns=True,mode='a')


def get_index_name_map():
    '''
    mapping_table에서　row_index에 사용될　name 가져오는　함수
    '''
    global diagnosis_output_path 

    store_diag = pd.HDFStore(diagnosis_output_path,mode='r')
    class_map_df = store_diag.select('metadata/mapping_table',columns=['class_code','mapping_code']).drop_duplicates()
    return class_map_df.set_index('mapping_code').to_dict()['class_code']


def get_diagnosis_df(no):
    '''
    환자번호를　넣으면　column은　KCDcode, row는　time-serial의　형태인　dataframe이　나오는　함수
    '''
    global diagnosis_output_path, KCD_USE_COLS, DIAG_TIME_INTERVAL
    
    store_diag = pd.HDFStore(diagnosis_output_path,mode='r')
    
    if not '/metadata/usecol' in store_diag.keys():
        set_diagnosis_row(); store_diag = pd.HDFStore(diagnosis_output_path,mode='r')

    col_list = get_timeseries_column()    
    # create empty dataframe 
    use_diagnosis_values = store_diag.select('metadata/usecol').col.values
    result_df = pd.DataFrame(columns=col_list,index=use_diagnosis_values)
    # target paitent dataframe
    target_df = store_diag.select('data',where='no=={}'.format(no))
    target_df = target_df.sort_values(['KCD_code','date'],axis=0)

    KCD_code_i = KCD_USE_COLS.index('KCD_code')
    date_i = KCD_USE_COLS.index('date')

    _prev_value = []
    for value in target_df.values:
        _,curr_code,curr_date = value
        if curr_code in use_diagnosis_values :

            if len(_prev_value)>0 and _prev_value[KCD_code_i] == curr_code:
                prev_date = _prev_value[date_i]

                if get_time_interval(prev_date,curr_date) <=DIAG_TIME_INTERVAL:
                    start_index = result_df.columns.get_loc(prev_date)
                    end_index = result_df.columns.get_loc(curr_date)
                    result_df.loc[int(value[KCD_code_i])].iloc[start_index:end_index] = 1

            result_df.loc[curr_code].loc[curr_date] = 1
        _prev_value = value

    del target_df

    index_name_dict = get_index_name_map()
    result_df.index = result_df.index.map(index_name_dict.get)
    return result_df
