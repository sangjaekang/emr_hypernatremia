#-*- encoding :utf-8 -*-
from .config import *
from .construct_common import check_directory, save_to_hdf5, get_timeseries_column, get_time_interval


def set_prescribe_row():
    global OFFSET_PRESCRIBE_COUNTS, PREP_OUTPUT_DIR, PRESCRIBE_OUTPUT_PATH
    PREP_OUTPUT_DIR= check_directory(PREP_OUTPUT_DIR)    
    prescribe_output_path = PREP_OUTPUT_DIR + PRESCRIBE_OUTPUT_PATH

    store_pres = pd.HDFStore(prescribe_output_path)
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


def get_index_name_map():
    global PREP_OUTPUT_DIR, PRESCRIBE_OUTPUT_PATH
    PREP_OUTPUT_DIR= check_directory(PREP_OUTPUT_DIR)    
    prescribe_output_path = PREP_OUTPUT_DIR + PRESCRIBE_OUTPUT_PATH

    store_pres = pd.HDFStore(prescribe_output_path)
    class_map_df=store_pres.select('metadata/mapping_table',columns=['ingd_name','mapping_code']).drop_duplicates()
    return class_map_df.set_index('mapping_code').to_dict()['ingd_name']


def get_prescribe_df(no):
    global PREP_OUTPUT_DIR, PRESCRIBE_OUTPUT_PATH, MEDI_USE_COLS
    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)
    prescribe_output_path = PREP_OUTPUT_DIR + PRESCRIBE_OUTPUT_PATH
    store_pres = pd.HDFStore(prescribe_output_path)

    if not '/metadata/usecol' in store_pres.keys():
        set_prescribe_row(); store_pres = pd.HDFStore(prescribe_output_path)

    col_list = get_timeseries_column()
    # create empty dataframe
    use_prescribe_values = store_pres.select('metadata/usecol').col.values
    result_df = pd.DataFrame(columns=col_list,index=use_prescribe_values)
    # target patient dataframe
    target_df = store_pres.select('data',where='no=={}'.format(no))

    medi_code_i = MEDI_USE_COLS.index('medi_code')
    date_i  = MEDI_USE_COLS.index('date')
    times_i = MEDI_USE_COLS.index('times')

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

