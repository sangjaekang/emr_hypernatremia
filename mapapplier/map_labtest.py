#-*- encoding :utf-8 -*-
from config import *
from map_common import convert_month, check_directory

def normalize_number(mean_x,min_x,max_x):
    '''
    dataframe 내 이상값을 전처리하는 함수.
    dataframe.map 을 이용할 것이므로, 함수 in 함수 구조 사용
    '''
    def _normalize_number(x):
        str_x = str(x).strip()

        re_num = re.compile('^[+-]?[\d]+[.]?[\d]*$')
        re_lower = re.compile('^<[\d\s]*[.]{0,1}[\d\s]*$')
        re_upper = re.compile('^>[\d\s]*[.]{0,1}[\d\s]*$')
        re_star = re.compile('^[\s]*[*][\s]*$')
        if re_num.match(str_x):
            return np.float(str_x)
        else:
            if re_lower.match(str_x):
                return np.float(0)
            elif re_upper.match(str_x):
                return  np.float(1)
            elif re_star.match(str_x):
                return np.float( (mean_x-min_x)/(max_x-min_x) )
            else:
                return np.nan

    return _normalize_number


def get_labtest_value(df,labtest_name):
    _temp = df[df.labtest.isin([labtest_name])]
    return _temp.avg.values[0], _temp.min.values[0], _temp.max.values[0]


def get_labtest_map():
    global LABMAP_OUTPUT_PATH, DELIM

    if not os.path.isfile(LABMAP_OUTPUT_PATH):
        raise ValueError("There is no labtest_OUTPUT file!")

    labtest_mapping_df = pd.read_csv(LABMAP_OUTPUT_PATH,delimiter=DELIM)

    return labtest_mapping_df


def run(labtest_data_path):
    global PER_LAB_DIR, PREP_OUTPUT_DIR, LAB_COL_NAME, USE_LAB_COL_NAME, LABTEST_OUTPUT_PATH, DEBUG_PRINT

    # syntax checking existence for directory
    PER_LAB_DIR = check_directory(PER_LAB_DIR)
    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)

    output_path = PREP_OUTPUT_DIR + LABTEST_OUTPUT_PATH

    # if the previous output file exists, remove it
    if os.path.isfile(output_path):    
        os.remove(output_path)
    # get mapping dataframe and save to hdf5 file 
    labtest_mapping_df = get_labtest_map()
    labtest_mapping_df = labtest_mapping_df.apply(pd.to_numeric,errors='ignore')
    per_lab_df.to_hdf(output_path, "map_df", format='table',date_columns=True, mode='a')

    re_per_lab = re.compile("^labtest_.*\.csv")     
    for file in os.path.listdir(PER_LAB_DIR):
        if re_per_lab.match(file):
            per_lab_name = file.replace('labtest_','').replace('.csv','')
            per_lab_path = PER_LAB_DIR + file
            per_lab_df = pd.read_csv(per_lab_path, delimiter = DELIM,
                                    names=LAB_COL_NAME, usecols=USE_LAB_COL_NAME)
            # 1. 값　가져오기
            r_avg, r_min, r_max = get_labtest_value(labtest_mapping_df, per_lab_name)
            per_lab_df.labtest =  per_lab_df.labtest.map(normalize_number(r_avg,r_min_r_max))
            per_lab_df.date = per_lab_df.date.map(convert_month) 
            # file type change
            per_lab_df = per_lab_df.apply(pd.to_numeric,errors='ignore')            
            per_lab_df.to_hdf(output_path, per_lab_name, format='table', date_columns=True, mode='a')

            if DEBUG_PRINT:
                print("{} dataframe enters hdf5 file".format(per_lab_name))


def set_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i",help="labtest data path")
    args = parser.parse_args()

    return args


if __name__=='__main__':
    args = set_parser()
    
    run(args.i)    