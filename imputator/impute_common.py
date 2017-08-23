#-*- encoding :utf-8 -*-
import sys, os, re
# 상위 폴더의 config를 import하기 위한 경로 설정
os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)
from imputator.config import *


def check_directory(directory_path):
    # syntax checking for directory
    if not (directory_path[-1] is '/'):
        directory_path  = directory_path + '/'
    # not exists in directory
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
    
    return directory_path        


def save_to_hdf5(input_path,output_path,key_name):
    #input_path에　있는　dataframe을　hdf5로　바꾸어주는　작업
    if os.path.isfile(output_path):
        os.remove(output_path)

    input_df = pd.read_csv(input_path,delimiter=DELIM)            
    input_df.to_hdf(output_path,key_name,format='table',data_columns=True,mode='a')
    del input_df


def check_time_format(x):
    # timeformat(ex: 20170801)의　형태로　되어있는지　확인하는　함수
    re_time = re.compile('^\d{8}$')
    if re_time.match(str(x)):
        return int(str(x)[2:6])
    else:
        raise ValueError("wrong type time format : {}".format(x))


def yield_time_column(time,end_t):
    # time-series column을　만드는데　있어서　순차적으로　생성하는　함수X
    # 1210 1211 1212 1301 1302 이런식으로　생성
    while time <= end_t:
        yield time
        year = time // 100 ; month = time % 100
        if month >= 12 :
            year = year + 1
            month = 1
        else : 
            month = month + 1
        time = (year*100 + month) 
        

def get_timeseries_column():
    global START_TIME, END_TIME
    start_time = check_time_format(START_TIME)
    end_time  = check_time_format(END_TIME)

    col_list = []
    for time in yield_time_column(start_time,end_time):
        col_list.append(time)

    return col_list


def get_time_interval(t_1,t_2):
    #t_1 t_2 두 시각의 시간 차 계산
    def _get_time(t):
        year = t//100; month = t%100
        return (year*12+month)
    if t_1<t_2:
        return _get_time(t_2)-_get_time(t_1)
    else :
        return _get_time(t_1)-_get_time(t_2)