#-*- encoding :utf-8 -*-
from config import *


def check_directory(directory_path):
    # syntax checking for directory
    if not (directory_path[-1] is '/'):
        directory_path  = directory_path + '/'

    # not exists in directory
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
    
    return directory_path        


def strip_space(x):
    # 띄어쓰기 날려버리는 함수
    if isinstance(x,str):
        return x.strip()
    else :
        return str(x).strip()


def convert_nan(df):
    # 빈 cell을 np.NaN으로 통일시키는 함수
    for column in df.columns:
        df[column] = df[column].map(strip_space,na_action='ignore')

    df = df.replace("",np.NaN)
    df = df.replace(".",np.NaN)
    df = df.replace("..",np.NaN)

    return df


def convert_date(date_type):
    '''
    date_type 
        1 : quarter
        0 : month
    '''
    get_year = lambda x : (x // 10000 % 100)
    get_quarter = lambda x : (x % 10000 // 100 - 1) //3 + 1
    re_date = re.compile('^\d{8}$') 

    def _convert_to_month(x):
        str_x = str(x)
        if re_date.match(str_x):
            return int(str_x[2:6])
        else : 
            raise ValueError("wrong number in date : {}".format(str_x))            
    
    def _convert_to_quarter(x):
        str_x = str(x)
        if re_date.match(str_x):
            return get_year(x) * 100 + get_quarter(x)
        else : 
            raise ValueError("wrong number in date : {}".format(str_x))
    
    if date_type is 1:
        return _convert_to_quarter
    else : 
        return _convert_to_month


re_date = re.compile('^\d{8}$') 

def convert_to_month(x):
    global re_date
    
    str_x = str(x)
    if re_date.match(str_x):
        return int(str_x[2:6])
    else : 
        raise ValueError("wrong number in date : {}".format(str_x))