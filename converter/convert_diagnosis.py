#-*- encoding :utf-8 -*-
import sys, os, re
# 상위 폴더의 config를 import하기 위한 경로 설정
os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)
from converter.config import *
from converter.convert_common import check_directory

def set_col_type(col_type):    
    if isinstance(col_type,str): 
        col_type = int(col_type)
    
    return {0: "대분류명",
                1: '중분류명',
                2:  '소분류명'}.get(x,'중분류명')


def preprocess_middle_class(df):
    df['중분류명'] = df['중분류명'].str.strip().replace('A92-A99',"A90-A99")
    df['중분류명'] = df['중분류명'].str.strip().replace('G10-G13',"G10-G14")    
    df['중분류명'] = df['중분류명'].str.strip().replace('K55-K63',"K55-K64")
    return df


def get_mapping_table(df,col_type):
    global MAPPING_DIR, KCD_MAPPING_PATH, DEBUG_PRINT

    MAPPING_DIR = check_directory(MAPPING_DIR)
    output_path = MAPPING_DIR + KCD_MAPPING_PATH

    code_to_class = pd.Series(df[col_type].values, index=df['진단용어코드'])
    class_to_int = pd.Series(range(1,len(df[col_type].unique())+1),index=df[col_type].unique())
    
    mapping_df = pd.concat([code_to_class,code_to_class.map(class_to_int)],
                axis=1,keys=code_to_class)

    mapping_df.index.name = 'KCD_code'
    mapping_df.columns = ['class_code','mapping_code']
    
    if DEBUG_PRINT:
        print(mapping_df.head())
    mapping_df.to_csv(output_path,sep=DELIM)
    del mapping_df


def run(col_type):
    global KCD_PATH
    '''
    input parameter
        col_type 
            0 : main category [대분류명]
            1 : middle class [중분류명]
            2 : sub class [소분류명]
    '''
    col_type = set_col_type(col_type)

    if not os.path.isfile(KCD_PATH):
        raise ValueError("WRONG KCD_PATH")
        
    KCD_df = pd.read_excel(KCD_PATH)    
    KCD_df = preprocess_middle_class(KCD_df)
    get_mapping_table(KCD_df,col_type)

    del KCD_df

def _set_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', help="select col_type (0 : 대분류명, 1 : 중분류명, 2 : 소분류명")
    args = parser.parse_args()
    return args


if __name__=='__main__':
    args = _set_parser()

    run(args.t)