# -*- coding :utf-8 -*-
import sys, os, re

os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)

from imputator.config import *
from imputator.impute_common import check_directory, get_timeseries_column, get_time_interval
from generator.generate_patient_emr import generate_emr

# output path setting
global PREP_OUTPUT_DIR, LABTEST_OUTPUT_PATH
PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)
labtest_output_path = PREP_OUTPUT_DIR + LABTEST_OUTPUT_PATH

'''
응급코드와　비응급코드는　결과값을　공유
|측정내용|응급코드|비응급코드|
|----|----|----|
|총단백|L8031|L3011|
|알부민|L8032|L3012|
|당정량|L8036|L3013|
|요소질소|L8037|L3018|
|크레이타닌|L8038|L3020|
|소디움|L8041|L3041|
|포타슘|L8042|L3042|
|염소 |L8043|L3043|
|혈액탄산|L8044|L3044|
|총칼슘|L8046|L3022|
|인   |L8047|L3023|
|요산|L8048|L3021|
|총콜레스테롤|L8049|L3013|
|중성지방|L8050|L3029|
|LDH|L8053|L3057|
'''

emgcy_and_not_dict = {
    'L8031' : 'L3011', 'L8032':'L3012',
    'L8036':'L3013',   'L8037':'L3018',
    'L8038':'L3020',   'L8041':'L3041',
    'L8042':'L3042',   'L8043':'L3043', 
    'L8044':'L3044',   'L8046':'L3022',
    'L8047':'L3023',   'L8048':'L3021',
    'L8049':'L3013',   'L8050':'L3029', 'L8053':'L3057'
}


def get_labtest_avg_map():
    global labtest_output_path

    if not os.path.isfile(labtest_output_path):
        raise ValueError("There is no labtest_OUTPUT file!")

    labtest_mapping_df = pd.HDFStore(labtest_output_path,mode='r').select('metadata/mapping_table').set_index('labtest')
    avg_map = (labtest_mapping_df.AVG -labtest_mapping_df.MIN)/(labtest_mapping_df.MAX-labtest_mapping_df.MIN)
    return avg_map


def _mean_with_nan(x,y):
    len_x = len(x.values)
    result_series = pd.Series(index=x.index)
    
    for i in range(len_x):
        if np.isnan(x.values[i]):
            if np.isnan(y.values[i]):
                result_series.iloc[i] = np.nan
            else:
                result_series.iloc[i] = y.values[i]
        else:
            if np.isnan(y.values[i]):
                result_series.iloc[i] = x.values[i]
            else:
                result_series.iloc[i] = np.mean((x.values[i],y.values[i]))
    return result_series


def _isn_nan(x):
    return not np.isnan(x)


def get_imputation_emr(df):
    emr_df = df.copy()
    global emgcy_and_not_dict

    lab_avg_map = get_labtest_avg_map()
    # 응급코드와　비응급코드의　수치를　평균해서　각각의　행에　재입력
    for emg,not_emg in emgcy_and_not_dict.items():
        avg_test = _mean_with_nan(emr_df.loc[emg],emr_df.loc[not_emg])
        emr_df.loc[emg] = avg_test
        emr_df.loc[not_emg] = avg_test

    # 하나　이상　측정한　경우가　있을　경우，　그것을　기준으로　linear interpolation으로　채워줌
    for inx in emr_df[emr_df.count(axis=1) != 0].index: 
        emr_series = emr_df.loc[inx]
        if emr_series.count() is 1:
            emr_series[:] = emr_series[emr_series.map(_isn_nan)].iloc[0]
        else :
            prev_date = None
            for date in emr_series[emr_series.map(_isn_nan)].index:
                if prev_date:
                    prev_val = emr_series.loc[prev_date]; curr_val = emr_series.loc[date]
                    inter_time = get_time_interval(prev_date,date)

                    for i in range(1,inter_time):
                        emr_series.loc[prev_date:date].iloc[i] = (curr_val-prev_val)/inter_time*i + prev_val                
                prev_date = date

            first_detect_index = emr_series[emr_series.map(_isn_nan)].index[0]
            emr_series.loc[:first_detect_index] = emr_series[emr_series.map(_isn_nan)].iloc[0] #앞부분　측정되지　않은건　제일　첫　측정된　값으로　대체
            last_detect_index = emr_series[emr_series.map(_isn_nan)].index[-1]
            emr_series.loc[last_detect_index:] = emr_series[emr_series.map(_isn_nan)].iloc[-1] #뒷부분　측정되지　않은건　제일　뒷　측정된　값으로　대체
    # 하나도 없을 경우, 평균으로 채워 넣음
    for inx in emr_df[emr_df.count(axis=1) == 0].index: 
        emr_df.loc[inx] = lab_avg_map[inx]

    return emr_df 


def get_boolmask_emr(emr_df):
    def _convert_nan(x):
        if np.isnan(x):
            return np.int(0)
        else : 
            return np.int(1)
    return emr_df.applymap(_convert_nan).copy()


def get_mean_emr(no):
    emr_df = generate_emr(no,{'lab'})

    bool_df = get_boolmask_emr(emr_df)
    imputed_df = get_imputation_emr(emr_df)

    result = np.stack((bool_df,imputed_df),axis=-1)
    result = result.astype(float)
    return return result