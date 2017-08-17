#-*- encoding :utf-8 -*-
from config import *
from map_common import convert_month, strip_space, check_directory, save_to_hdf5

def save_mapping_to_hdf5():
    global MAPPING_DIR, KCD_MAPPING_PATH, PREP_OUTPUT_DIR, DIAGNOSIS_OUTPUT_PATH

    MAPPING_DIR = check_directory(MAPPING_DIR)
    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)

    KCD_output_path = MAPPING_DIR + KCD_MAPPING_PATH 
    diagnosis_output_path = PREP_OUTPUT_DIR + DIAGNOSIS_OUTPUT_PATH

    if not os.path.isfile(KCD_output_path):
        raise ValueError("There is no KCD_OUTPUT file!")

    save_to_hdf5(KCD_output_path, diagnosis_output_path,'metadata/mapping_table')

def get_diagnosis_map():
    global MAPPING_DIR, KCD_MAPPING_PATH

    MAPPING_DIR = check_directory(MAPPING_DIR)
    KCD_output_path = MAPPING_DIR + KCD_MAPPING_PATH 

    if not os.path.isfile(KCD_output_path):
        raise ValueError("There is no KCD_OUTPUT file!")

    KCD_df = pd.read_csv(KCD_output_path,delimiter=DELIM)
    KCD_to_code = pd.Series(KCD_df.mapping_code.values,index=KCD_df.KCD_code.values).to_dict()

    del KCD_df

    return KCD_to_code


def run(diagnosis_data_path):
    global DELIM, KCD_COL_NAME, KCD_USE_COLS, CHUNK_SIZE, DIAGNOSIS_OUTPUT_PATH,DEBUG_PRINT,TEMP_PATH, PREP_OUTPUT_DIR

    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)
    diagnosis_output_path = PREP_OUTPUT_DIR + DIAGNOSIS_OUTPUT_PATH

    
    KCD_to_code = get_diagnosis_map() # mapping dictionary

    if os.path.isfile(TEMP_PATH):
        raise ValueError("data Corruption WARNING! --> maybe other process using TEMP file ")

    chunks = pd.read_csv(diagnosis_data_path, delimiter = DELIM, 
                header=None, names=KCD_COL_NAME, usecols=KCD_USE_COLS, chunksize=CHUNK_SIZE)
    for idx, chunk in enumerate(chunks):
        #### mapping
        chunk.KCD_code = chunk.KCD_code.map(strip_space)
        chunk.KCD_code = chunk.KCD_code.map(KCD_to_code)
        chunk.date = chunk.date.map(convert_month)

        if idx is 0:
            chunk.to_csv(TEMP_PATH, sep=DELIM, header=KCD_USE_COLS,index=False)
        else:
            chunk.to_csv(TEMP_PATH, sep=DELIM, header=False,index=False,mode='a')
        
        if DEBUG_PRINT:
            print('{} th chunk of output enters temp file'.format(idx))

    save_to_hdf5(TEMP_PATH, diagnosis_output_path,'data')
    os.remove(TEMP_PATH) # temp file remove
    save_mapping_to_hdf5()


def set_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i",help="diagnosis data path")
    args = parser.parse_args()

    return args


if __name__=='__main__':
    args = set_parser()
    
    run(args.i)    