# -*- coding: utf-8 -*-
import sys, os, re

os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)
from mapapplier.config import *
from mapapplier.map_common import convert_month, check_directory, save_to_hdf5

def save_mapping_to_hdf5():
    global MAPPING_DIR, MEDICINE_MAPPING_PATH,PREP_OUTPUT_DIR,PRESCRIBE_OUTPUT_PATH
    
    MAPPING_DIR = check_directory(MAPPING_DIR)
    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)

    prescribe_output_path = PREP_OUTPUT_DIR + PRESCRIBE_OUTPUT_PATH
    medicine_mapping_path = MAPPING_DIR + MEDICINE_MAPPING_PATH

    if not os.path.isfile(medicine_mapping_path):
        raise ValueError("There is no medicine_mapping dataframe!")

    save_to_hdf5(medicine_mapping_path,prescribe_output_path,'metadata/mapping_table')


def get_prescribe_map():
    global MAPPING_DIR, MEDICINE_MAPPING_PATH
    MAPPING_DIR = check_directory(MAPPING_DIR)
    medicine_mapping_path = MAPPING_DIR + MEDICINE_MAPPING_PATH

    if not os.path.isfile(medicine_mapping_path):
        raise ValueError("There is no medicine_mapping dataframe!")

    prescribe_map_df = pd.read_csv(medicine_mapping_path,delimiter=DELIM)
    mapping_dict = pd.Series(prescribe_map_df.mapping_code.values ,index=prescribe_map_df.medi_code ).to_dict()
    
    del prescribe_map_df
    return mapping_dict


re_date = re.compile('^\d{8}$')
def check_not_date_type(x):
    global re_date
    str_x = str(x)
    return not bool(re_date.match(str_x))


def convert_times_per_month(x):
    return float(x) // 30


def run(prescribe_lab_path):
    global DELIM, CHUNK_SIZE, PRESCRIBE_OUTPUT_PATH, PREP_OUTPUT_DIR,TEMP_PATH
    
    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)
    prescribe_output_path = PREP_OUTPUT_DIR + PRESCRIBE_OUTPUT_PATH
    
    mapping_dict = get_prescribe_map() # mapping dictionary

    if os.path.isfile(TEMP_PATH):
        raise ValueError("data Corruption WARNING! --> maybe other process using TEMP file ")

    chunks = pd.read_csv(prescribe_lab_path, delimiter=DELIM, chunksize=CHUNK_SIZE)
    for idx, chunk in enumerate(chunks):
        #### 임시　코드 start###
        chunk.drop(chunk[chunk.date.map(check_not_date_type)].index,inplace=True)
        chunk.drop(['medi_name','date1'],axis=1,inplace=True)
        #### 임시　코드  end###
        chunk['medi_code'] = chunk['medi_code'].map(mapping_dict)
        chunk['date']           = chunk['date'].map(convert_month)
        chunk['times'] = chunk['times'].map(convert_times_per_month) 
        if idx is 0:
            chunk.to_csv(TEMP_PATH, sep=DELIM, index=False)
        else : 
            chunk.to_csv(TEMP_PATH, sep=DELIM, index=False, header=False, mode='a')
        if DEBUG_PRINT:
            print('{} th chunk of output enters temp file'.format(idx))
    
    save_to_hdf5(TEMP_PATH, prescribe_output_path,'data')    
    os.remove(TEMP_PATH) # temp file remove
    save_mapping_to_hdf5()


def set_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i",help="prescribe data path")
    args = parser.parse_args()

    return args


if __name__=='__main__':
    args = set_parser()

    run(args.i)