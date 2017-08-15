import pandas as pd
import numpy as np
import re
import sys
import os
import argparse
import h5py

# 1. related to computer spec 

CHUNK_SIZE = 50000000

# 2. related to data spec 

PATIENT_NUM = 5400000 # the number of patients
KCD_COL_NAME = ['no','KCD_code','context','context_check','date']
KCD_USE_COLS = ['no','KCD_code','date']
DELIM = ',' # dataÀÇ delimiter / sep ¿¡ ÇØ´çÇÏ´Â »ó¼ö
DROP_RATE = 10000 # the rate value to determine whether code is drop or not

LAB_COL_NAME = ['no','lab_code','date','result']
USE_LAB_COL_NAME = ['no','date','result']


PRESCRIBE_COL_NAME = ['환자번호', '약품코드', '약품명' , '검사일자' , '처방일자 ' , 
'ATC분류코드' ,   'ATC분류설명' ,   'ASHP분류코드' ,   'ASHP분류설명' ,   '복지부분류코드' ,   
'복지부분류설명' ,   '총용량' ,   '투여량' ,   '횟수' ,   '일수']

# 3. related to TEMP_FILE path

PREP_OUTPUT_DIR = '../data/prep/'

KCD_OUTPUT_PATH = '../data/KCD_mapping_df.csv'
DIAGNOSIS_OUTPUT_PATH = '../data/pre_diagnosis_df.csv'
KCD_COUNTS_PATH = '../data/KCD_count_df.csv'

PER_LAB_DIR      = '../data/per_lab/'
LABMAP_OUTPUT_PATH = '../data/labtest_mapping_df.csv'
LABTEST_OUTPUT_PATH = 'pre_labtest_df.h5'


MEDICINE_OUTPUT_PATH = '../data/medicine_mapping_df.csv'
PRESCRIBE_OUTPUT_PATH = '../data/pre_prescribe_df.csv'

# 4. related to DEBUG

DEBUG_PRINT = True