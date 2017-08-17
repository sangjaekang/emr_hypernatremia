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


def save_to_hdf5(input_path,output_path,key_name):
    if os.path.isfile(output_path):
        os.remove(output_path)

    input_df = pd.read_csv(input_path,delimiter=DELIM)            
    input_df.to_hdf(output_path,key_name,format='table',data_columns=True,mode='a')
    del input_df


def check_time_format(x):
    re_time = re.compile('^\d{8}$')
    if re_time.match(str(x)):
        return int(str(x)[2:6])
    else:
        raise ValueError("wrong type time format : {}".format(x))


def yield_time_column(time,end_t):
    while time <= end_t:
        year = time // 100 ; month = time % 100
        if month >= 12 :
            year = year + 1
            month = 1
        else : 
            month = month + 1
        time = (year*100 + month) 
        yield time


def get_timeseries_column():
    global START_TIME, END_TIME
    start_time = check_time_format(START_TIME)
    end_time  = check_time_format(END_TIME)

    col_list = []
    for time in yield_time_column(start_time,end_time):
        col_list.append(time)

    return col_list

def get_time_interval(t_1,t_2):
    def _get_time(t):
        year = t//100; month = t%100
        return (year*12+month)
    if t_1<t_2:
        return _get_time(t_2)-_get_time(t_1)
    else :
        return _get_time(t_1)-_get_time(t_2)