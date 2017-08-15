#-*- encoding :utf-8 -*-
from config import *
from map_common import convert_month, check_directory


def get_prescribe_map():
    global MEDICINE_OUTPUT_PATH

    if not os.path.isfile(MEDICINE_OUTPUT_PATH):
        raise ValueError("There is no medicine_mapping dataframe!")

    prescribe_map_df = pd.read_csv(MEDICINE_OUTPUT_PATH,delimiter=DELIM)
    mapping_dict = pd.Series(prescribe_map_df.mapping_code.values ,index=prescribe_map_df.medi_code ).to_dict()
    
    del prescribe_map_df
    return mapping_dict


re_date = re.compile('^\d{8}$')
def check_not_date_type(x):
    global re_date
    str_x = str(x)
    return not bool(re_date.match(str_x))


def run(prescribe_lab_path):
    global DELIM, CHUNK_SIZE, PRESCRIBE_OUTPUT_PATH
    
    mapping_dict = get_prescribe_map()

    chunks = pd.read_csv(prescribe_lab_path, delimiter=DELIM, chunksize=CHUNK_SIZE)
    for idx, chunk in enumerate(chunks):
        #### 임시　코드 start###
        chunk.drop(chunk[chunk.date.map(check_not_date_type)].index,inplace=True)
        chunk.drop(['medi_name','date1'],axis=1,inplace=True)
        #### 임시　코드  end###
        chunk['medi_code'] = chunk['medi_code'].map(mapping_dict)
        chunk['date']           = chunk['date'].map(convert_month)
        if idx is 0:
            chunk.to_csv(PRESCRIBE_OUTPUT_PATH, sep=DELIM, index=False)
        else : 
            chunk.to_csv(PRESCRIBE_OUTPUT_PATH, sep=DELIM, index=False, header=False, mode='a')
        

def set_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i",help="prescribe data path")
    args = parser.parse_args()

    return args


if __name__=='__main__':
    args = set_parser()

    run(args.i)