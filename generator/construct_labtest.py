# -*- coding: utf-8 -*-
import sys, os, re

os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)
from generator.config import *
from generator.construct_common import check_directory, save_to_hdf5, get_timeseries_column, get_time_interval

# output path setting
global PREP_OUTPUT_DIR, LABTEST_OUTPUT_PATH
PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)
labtest_output_path = PREP_OUTPUT_DIR + LABTEST_OUTPUT_PATH


def set_labtest_row():
    '''
    lab_test name를 row_index_name으로나열
    OFFSET_labtest_COUNTS 기준에　따라서，drop 할　row을　결정
    drop하고　남은　row를　metadata/usecol에　저장
    labtest는　labtest 별로　hdf5에　따로　저장되어　있음
    그래서　get_index_map으로　부르지않음
    '''
    global labtest_output_path
    store_lab = pd.HDFStore(labtest_output_path,mode='r')

    data_node = store_lab.get_node('data')
    if not data_node:
        raise ValueError("There is no data in labtest data")

    use_index_df = pd.DataFrame(columns=['col','value_counts'])
    lab_node = store_lab.get_node('data')
    for lab_name in lab_node._v_children.keys():
        value_counts = store_lab.select('data/{}'.format(lab_name)).shape[0]
        use_index_df = use_index_df.append({'col':lab_name,'value_counts':value_counts},ignore_index=True)

    total_counts = use_index_df['value_counts'].sum()
    min_value_counts = total_counts * OFFSET_LABTEST_COUNTS

    use_index_df = use_index_df[use_index_df['value_counts'] > min_value_counts]
    use_index_df['value_counts'] = pd.to_numeric(use_index_df['value_counts'])
    store_lab.close()
    use_index_df.to_hdf(labtest_output_path,'metadata/usecol',format='table',data_columns=True,mode='a')


def get_labtest_df(no): 
    '''
    환자번호를　넣으면　column은　KCDcode, row는　time-serial의　형태인　dataframe이　나오는　함수
    '''
    global labtest_output_path
    store_lab = pd.HDFStore(labtest_output_path,mode='r')
    
    if not '/metadata/usecol' in store_lab.keys():
        store_lab.close()
        set_labtest_row(); 
    else:
        store_lab.close()
    store_lab = pd.HDFStore(labtest_output_path,mode='r')

    col_list = get_timeseries_column()
    # create empty dataframe
    use_labtest_values = store_lab.select('metadata/usecol').col.values
    result_df = pd.DataFrame(columns=col_list, index=use_labtest_values)

    lab_node = store_lab.get_node('data')
    for lab_name in lab_node._v_children.keys():
        result_lab_series = result_df.loc[lab_name]
        target_df = store_lab.select('data/{}'.format(lab_name),where='no=={}'.format(no))
        target_df = target_df.groupby(['no','date']).mean().reset_index() # 같은달에 한번 이상 했을 시, 결과의 평균으로 저장
        
        pre_value = None
        for value in target_df.values:
            _, _date, _result = value
            result_df.loc[lab_name].loc[_date] = _result
            # if pre_value is not None:
            #     min_time = pre_value[1];max_time =_date
            #     min_value = pre_value[2];max_value = _result
            #     inter_time = get_time_interval(min_time,max_time)
            #     for i in result_lab_series.loc[pre_value[1]:_date].index:
            #         intpol_time = get_time_interval(min_time,i) / inter_time
            #         intpol_value = (max_value-min_value)*intpol_time + min_value
            #         result_df.loc[lab_name].loc[i] = intpol_value
            #     pre_value = value
            # else :
            #     result_df.loc[lab_name].loc[:_date] = _result
            #     pre_value = value

        # missing_index = result_lab_series[result_lab_series.isnull()].index
        # if missing_index.min() > col_list[0]:
        #     last_loc = result_df.columns.get_loc(missing_index.min()) - 1
        #     result_df.loc[lab_name].iloc[last_loc+1:]= result_df.loc[lab_name].iloc[last_loc]

    del target_df
    store_lab.close()
    return result_df


