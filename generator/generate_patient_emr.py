#-*- encoding :utf-8 -*-
import sys, os, re
# 상위 폴더의 config를 import하기 위한 경로 설정
os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)
from generator.config import *

from generator.consturct_demographic import get_demo_df
from generator.construct_diagnosis import get_diagnosis_df
from generator.construct_labtest import get_labtest_df
from generator.construct_prescribe import get_prescribe_df


def generate_emr(no):
    per_demo_df = get_demo_df(no)
    per_diag_df = get_diagnosis_df(no)
    per_labtest_df = get_labtest_df(no)
    per_pres_df = get_prescribe_df(no)

    emr_df = pd.concat([per_demo_df,per_diag_df,per_labtest_df,per_pres_df])

    return emr_df