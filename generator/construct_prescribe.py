# -*- coding: utf-8 -*-
import sys, os, re

os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)
from generator.config import *
from generator.construct_common import check_directory, save_to_hdf5, get_timeseries_column, get_time_interval

# output path setting
global PREP_OUTPUT_DIR, PRESCRIBE_OUTPUT_PATH
PREP_OUTPUT_DIR= check_directory(PREP_OUTPUT_DIR)    
prescribe_output_path = PREP_OUTPUT_DIR + PRESCRIBE_OUTPUT_PATH


def set_prescribe_row():
    '''
    약품코드를row_index_name으로나열
    OFFSET_PRESICRIBE_COUNTS 기준에　따라서，drop 할　row을　결정
    drop하고　남은　row를　metadata/usecol에　저장
    '''
    global OFFSET_PRESCRIBE_COUNTS, prescribe_output_path

    store_pres = pd.HDFStore(prescribe_output_path,mode='r')
    if not '/data' in store_pres.keys():
        raise ValueError("There is no data in prescribe data")

    pres_df = store_pres.select('data')
    value_counts_diag = pres_df['medi_code'].value_counts()
    total_pres =  pres_df['medi_code'].sum()
    min_value_counts = int(total_pres * OFFSET_PRESCRIBE_COUNTS)

    use_index_df = value_counts_diag[value_counts_diag>min_value_counts].sort_index().reset_index()
    use_index_df.columns = ['col','value_counts']
    store_pres.close()
    use_index_df.to_hdf(prescribe_output_path,'metadata/usecol',format='table',data_columns=True,mode='a')
    del use_index_df

def get_index_name_map():
    '''
    mapping_table에서　row_index에 사용될　name 가져오는　함수
    '''
    global prescribe_output_path

    store_pres = pd.HDFStore(prescribe_output_path,mode='r')
    class_map_df=store_pres.select('metadata/mapping_table',columns=['ingd_name','mapping_code']).drop_duplicates()
    return class_map_df.set_index('mapping_code').to_dict()['ingd_name']


def get_prescribe_df(no):
    '''
    환자번호를　넣으면　column은　KCDcode, row는　time-serial의　형태인　dataframe이　나오는　함수
    '''
    global prescribe_output_path, MEDI_USE_COLS
    store_pres = pd.HDFStore(prescribe_output_path,mode='r')

    if not '/metadata/usecol' in store_pres.keys():
        set_prescribe_row(); store_pres = pd.HDFStore(prescribe_output_path,mode='r')

    col_list = get_timeseries_column()
    # create empty dataframe
    use_prescribe_values = store_pres.select('metadata/usecol').col.values
    result_df = pd.DataFrame(columns=col_list,index=use_prescribe_values)
    # target patient dataframe
    target_df = store_pres.select('data',where='no=={}'.format(no))

    for value in target_df.values:
        _ , _medi_code, _date, _times = value
        if _medi_code in use_prescribe_values:
            start_index = result_df.columns.get_loc(_date)
            end_index = start_index + _times + 1
            result_df.loc[_medi_code].iloc[start_index:end_index] = 1

    del target_df

    index_name_dict = get_index_name_map()
    result_df.index = result_df.index.map(index_name_dict.get)
    return result_df

