import pandas as pd
import numpy as np
import re
import sys
import os
import argparse

#-*-encoding:utf-8-*-
# 1. related to computer spec 

CHUNK_SIZE = 50000000
NUM_CORES = 4 # multi-processing

# 2. related to data spec 

DELIM = ',' 

KCD_COL_NAME = ['no','KCD_code','context','context_check','date']
KCD_USE_COLS = ['no','KCD_code','date']

LAB_COL_NAME = ['no','lab_code','date','result']
USE_LAB_COL_NAME = ['no','date','result']
MAP_LAB_COL_NAME = ['labtest','AVG','MIN','MAX']

MEDI_COL_NAME = ['약품코드', '약품명', '시작일자', '종료일자', '성분명', 'ATC분류코드', 'ATC분류설명']

DEMO_COL_NAME = ['no','sex','age']

# 3. related to TEMP_FILE path

## data directory
PREP_OUTPUT_DIR = '../data/prep/'
MAPPING_DIR = '../data/map_table/'
PER_LAB_DIR      = '../data/per_lab/'

## convert mapping input data path
KCD_PATH = '../data/KCD.xlsx'
MEDICINE_CONTEXT_PATH = '../data/medicine_context.xlsx'

## mapping dataframe path 
KCD_MAPPING_PATH = 'KCD_mapping_df.csv'
LAB_MAPPING_PATH = 'labtest_mapping_df.csv'
MEDICINE_MAPPING_PATH = 'medicine_mapping_df.csv'

## preprocess_data_path
LABTEST_OUTPUT_PATH = 'pre_labtest_df.h5'
PRESCRIBE_OUTPUT_PATH = 'pre_prescribe_df.h5'
DIAGNOSIS_OUTPUT_PATH = 'pre_diagnosis_df.h5'
DEMOGRAPHIC_OUTPUT_PATH = 'pre_demographic_df.h5'

## TEMP file path
TEMP_PATH = 'temp_output.csv'


# 4. related to DEBUG

DEBUG_PRINT = True