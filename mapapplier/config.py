import pandas as pd
import numpy as np
import re
import sys
import os
import argparse

# 1. related to computer spec 

CHUNK_SIZE = 50000000

# 2. related to data spec 

PATIENT_NUM = 5400000 # the number of patients
KCD_COL_NAME = ['no','KCD_code','context','context_check','date']
KCD_USE_COLS = ['no','KCD_code','date']
DELIM = ',' # dataÀÇ delimiter / sep ¿¡ ÇØ´çÇÏ´Â »ó¼ö
DROP_RATE = 10000 # the rate value to determine whether code is drop or not

LAB_COL_NAME = ['no','lab_code','date','result']


# 3. related to TEMP_FILE path

KCD_OUTPUT_PATH = '../data/KCD_mapping_df.csv'
DIAGNOSIS_OUTPUT_PATH = '../data/pre_diagnosis_df.csv'
KCD_COUNTS_PATH = '../data/KCD_count_df.csv'

LABMAP_OUTPUT_PATH = '../data/labtest_mapping_df.csv'
LABTEST_OUTPUT_PATH = '../data/pre_labtest_df.csv'

# 4. related to DEBUG

DEBUG_PRINT = True