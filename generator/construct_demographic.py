#-*- encoding :utf-8 -*-
import sys, os, re
# 상위 폴더의 config를 import하기 위한 경로 설정
os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)
from generator.config import *
from generator.construct_common import check_directory, save_to_hdf5, get_timeseries_column

# output path setting
global PREP_OUTPUT_DIR, DEMOGRAPHIC_OUTPUT_PATH
PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)
demographic_output_path = PREP_OUTPUT_DIR + DEMOGRAPHIC_OUTPUT_PATH


def set_age_dummies():
    # 나이대를　설정해주는　함수
    # main config파일에　어떻게　나이대를　설정할　것인지　나타나있음（AGE_BREAK_POINTS)
    global demographic_output_path, AGE_BREAK_POINTS, AGE_LABELS
    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)
    demographic_output_path = PREP_OUTPUT_DIR + DEMOGRAPHIC_OUTPUT_PATH

    store_demo = pd.HDFStore(demographic_output_path)

    demo_except_age = store_demo.select('data/original',columns=['no','sex'])
    demo_age = store_demo.select('data/original',columns=['age'])

    cat_demo_age = pd.cut(demo_age.age, AGE_BREAK_POINTS, labels = AGE_LABELS)
    cat_demo_age = cat_demo_age.cat.add_categories(['not known'])
    cat_demo_age[cat_demo_age.isnull()] = 'not known'
    cat_demo_age = pd.get_dummies(cat_demo_age)

    _df = pd.concat([demo_except_age, cat_demo_age],axis=1)
    store_demo.close()
    _df.to_hdf(demographic_output_path,'data/dummy',format='table',data_columns=True,mode='a')


def get_demographic_series(no):
    global demographic_output_path

    store_demo = pd.HDFStore(demographic_output_path)

    # if there is no dummy dataframe, set it
    if not '/data/dummy' in store_demo.keys():
        set_age_dummies(); store_demo.open()

    return store_demo.select('/data/dummy',where='no=={}'.format(no)).squeeze().drop('no')

def get_demo_df(no):
    per_demo_series = get_demographic_series(no)
    per_demo_df = pd.DataFrame(index=per_demo_series.index, columns=get_timeseries_column())
    
    for column in per_demo_df.columns:
        per_demo_df[column] = per_demo_series

    return per_demo_df