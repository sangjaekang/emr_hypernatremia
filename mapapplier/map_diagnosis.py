#-*- encoding :utf-8 -*-
from config import *
from map_common import convert_month, strip_space

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
    global DELIM, KCD_COL_NAME, KCD_USE_COLS, CHUNK_SIZE, DIAGNOSIS_OUTPUT_PATH

    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)
    diagnosis_output_path = PREP_OUTPUT_DIR + DIAGNOSIS_OUTPUT_PATH

    chunks = pd.read_csv(diagnosis_data_path, delimiter = DELIM, 
                header=None, names=KCD_COL_NAME, usecols=KCD_USE_COLS, chunksize=CHUNK_SIZE)
    
    KCD_to_code = get_diagnosis_map() # mapping dictionary
    for idx, chunk in enumerate(chunks):
        #### mapping
        chunk.KCD_code = chunk.KCD_code.map(strip_space)
        chunk.KCD_code = chunk.KCD_code.map(KCD_to_code)
        chunk.date = chunk.date.map(convert_month)
        
        if idx is 0:
            chunk.to_csv(diagnosis_output_path, sep=DELIM, header=KCD_USE_COLS,index=False)
        else:
            chunk.to_csv(diagnosis_output_path, sep=DELIM, header=False,index=False,mode='a')

'''
def count():
    global DELIM, CHUNK_SIZE, DIAGNOSIS_OUTPUT_PATH, KCD_COUNTS_PATH, PREP_OUTPUT_DIR

    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)
    diagnosis_output_path = PREP_OUTPUT_DIR + DIAGNOSIS_OUTPUT_PATH

    chunks = pd.read_csv(diagnosis_output_path, delimiter = DELIM, chunksize=CHUNK_SIZE)

    result = pd.concat([pd.value_counts(chunk.KCD_code.values,sort=False) for chunk in chunks])
    result = result.groupby(result.index).sum()

    result.to_csv(KCD_COUNTS_PATH, sep=DELIM, index=True)


def drop_useless_data():
    global KCD_COUNTS_PATH, DELIM, CHUNK_SIZE, DROP_RATE

    KCD_count_df = pd.read_csv(KCD_COUNTS_PATH,names=['mapping_code','count'], delimiter=DELIM)
    KCD_mapping_df = pd.read_csv(KCD_MAPPING_PATH,delimiter=DELIM)

    int_to_code = pd.Series(KCD_mapping_df.class_code.unique(),index = KCD_mapping_df.mapping_code.unique()).to_dict()

    KCD_count_df['KCD_code'] = KCD_count_df['mapping_code'].map(int_to_code)

    cutoff_count = KCD_count_df['count'].sum() // DROP_RATE
    use_df  = KCD_count_df[KCD_count_df['count']>=cutoff_count]

    KCD_mapping_df = KCD_mapping_df[KCD_mapping_df.mapping_code.isin(use_df.mapping_code)]
    KCD_mapping_df.to_csv(KCD_MAPPING_PATH,sep=DELIM)
    del KCD_mapping_df

    chunks = pd.read_csv(DIAGNOSIS_OUTPUT_PATH,delimiter=DELIM,chunksize=CHUNK_SIZE)
    for idx, chunk in enumerate(chunks):
        temp = chunk[chunk.KCD_code.isin(use_df.mapping_code.values)]
        if idx is 0:
            temp.to_csv(TEMP_PATH, sep=DELIM, index=False)
        else:
            temp.to_csv(TEMP_PATH, sep=DELIM, header=False,index=False,mode='a')

    if os.path.isfile(DIAGNOSIS_OUTPUT_PATH):
        os.remove(DIAGNOSIS_OUTPUT_PATH)

    os.rename(TEMP_PATH,DIAGNOSIS_OUTPUT_PATH)
'''

def set_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i",help="diagnosis data path")
    args = parser.parse_args()

    return args


if __name__=='__main__':
    args = set_parser()
    
    run(args.i)    