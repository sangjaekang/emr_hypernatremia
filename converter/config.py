import pandas as pd
import numpy as np
import re
import sys
import os
import argparse

# 1. related to computer spec 

CHUNK_SIZE = 10000000

# 2. related to data spec

COL_NAME = ['no','lab_code','date','result']
DELIM = ','

# 3. related to TEMP_FILE path

KCD_PATH = '../data/KCD.xlsx'
KCD_OUTPUT_PATH = '../data/KCD_mapping_df.csv'

PER_LAB_DIR      = '../data/per_lab/'
LAB_OUTPUT_PATH = '../data/labtest_mapping_df.csv'

# 4. related to DEBUG

DEBUG_PRINT = True