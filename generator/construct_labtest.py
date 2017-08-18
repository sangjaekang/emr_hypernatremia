#-*- encoding :utf-8 -*-
from .config import *
from .construct_common import check_directory, save_to_hdf5, get_timeseries_column, get_time_interval

def set_labtest_row():
    global PREP_OUTPUT_DIR, LABTEST_OUTPUT_PATH, OFFSET_LABTEST_COUNTS
    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)
    labtest_output_path = PREP_OUTPUT_DIR + LABTEST_OUTPUT_PATH
    store_lab = pd.HDFStore(labtest_output_path)

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
    global PREP_OUTPUT_DIR, LABTEST_OUTPUT_PATH
    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR) 
    labtest_output_path = PREP_OUTPUT_DIR + LABTEST_OUTPUT_PATH
    store_lab = pd.HDFStore(labtest_output_path)
    
    if not '/metadata/usecol' in store_lab.keys():
        set_labtest_row(); store_lab = pd.HDFStore(labtest_output_path)

    col_list = get_timeseries_column()
    # create empty dataframe
    use_labtest_values = store_lab.select('metadata/usecol').col.values
    result_df = pd.DataFrame(columns=col_list, index=use_labtest_values)
    
    lab_node = store_lab.get_node('data')
    for lab_name in lab_node._v_children.keys():
        target_df = store_lab.select('data/{}'.format(lab_name),where='no=={}'.format(no))
        for value in target_df.values:
            _, _date, _result = value
            result_df.loc[lab_name].loc[_date] = _result

    del target_df

    return result_df


