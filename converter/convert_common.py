# -*- coding: utf-8 -*-
import sys, os, re

os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)
from converter.config import *


def check_df_size(df_path):
    # 우리가 읽을 dataframe의 사이즈를 체크
    global  DELIM

    if not os.path.isfile(df_path):
        raise ValueError("no lab test data path")

    f = open(df_path,mode='r',encoding="utf-8")
    col_num = f.readline().count(DELIM)+1
    row_num = sum(1 for _ in f)

    print("dataframe의 (row,col) : ({},{}) ".format(row_num,col_num))    


def check_directory(directory_path):
    # syntax checking for directory
    if not (directory_path[-1] is '/'):
        directory_path  = directory_path + '/'
    # not exists in directory
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
    
    return directory_path        