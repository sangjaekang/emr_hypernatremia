import pandas as pd
import numpy as np
import re
import sys
import os
import argparse

# 1. related to computer spec 

CHUNK_SIZE = 10000000

# 2. related to data spec

LAB_COL_NAME = ['no','lab_code','date','result']
USE_LAB_COL_NAME = ['no','date','result']
DELIM = ','

MEDI_COL_NAME = ['약품코드', '약품명', '시작일자', '종료일자', '성분명', 'ATC분류코드', 'ATC분류설명']

MAP_LAB_COL_NAME = ['labtest','AVG','MIN','MAX']

# 3. related to TEMP_FILE path

KCD_PATH = '../data/KCD.xlsx'
KCD_OUTPUT_PATH = '../data/KCD_mapping_df.csv'

PER_LAB_DIR      = '../data/per_lab/'
LAB_OUTPUT_PATH = '../data/labtest_mapping_df.csv'

MEDICINE_CONTEXT_PATH = '../data/medicine_context.xlsx'
MEDICINE_OUTPUT_PATH = '../data/medicine_mapping_df.csv'

# 4. related to DEBUG
DEBUG_PRINT = True