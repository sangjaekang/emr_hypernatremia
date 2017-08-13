# 1. related to computer spec 

CHUNK_SIZE = 50000000
DELIM = ','
COL_NAME = ['no','KCD_code','context','context_check','date']
USE_COLS = ['no','KCD_code','date']

# 2. related to data spec 

PATIENT_NUM = 5400000 # the number of patients
DELIM = ',' # dataÀÇ delimiter / sep ¿¡ ÇØ´çÇÏ´Â »ó¼ö
DROP_RATE = 10000 # the rate value to determine whether code is drop or not

# 3. related to TEMP_FILE path

KCD_OUTPUT_PATH = '../data/KCD_mapping_df.csv'
DIAGNOSIS_OUTPUT_PATH = '../data/pre_prescribe_df.csv'
KCD_COUNTS_PATH = '../data/KCD_count_df.csv'
TEMP_PATH = '../data/TEMP_df.csv'
