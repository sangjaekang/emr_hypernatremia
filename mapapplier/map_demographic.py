#-*- encoding :utf-8 -*-
from .config import *
from .map_common import check_directory, save_to_hdf5


def check_age(x):
    if type(x) is not float:
        return np.nan
    elif x<0.0 : 
        return np.nan
    else:
        return x


def run(demographic_path):
    global PREP_OUTPUT_DIR, DEMOGRAPHIC_OUTPUT_PATH, TEMP_PATH

    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)
    demographic_output_path = PREP_OUTPUT_DIR + DEMOGRAPHIC_OUTPUT_PATH

    if os.path.isfile(TEMP_PATH):
        raise ValueError("data Corruption WARNING! --> maybe other process using TEMP file ")
        
    demographic_df = pd.read_excel(demographic_path)
    demographic_df.columns = DEMO_COL_NAME
    sex_dict = {'F':1,'M':0}
    demographic_df['sex'] = demographic_df['sex'].map(sex_dict)
    demographic_df['age'] = demographic_df['age'].map(check_age)

    demographic_df.to_csv(TEMP_PATH,sep=DELIM, index=False)

    save_to_hdf5(TEMP_PATH, demographic_output_path, 'data/original')
    os.remove(TEMP_PATH)
    

def set_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i",help="demographic data path")
    args = parser.parse_args()

    return args

if __name__ == '__main__':
    args = set_parser()

    run(args.i)