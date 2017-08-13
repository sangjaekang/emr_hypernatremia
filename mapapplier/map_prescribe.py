#-*- encoding :utf-8 -*-
import pandas as pd
import numpy as np
import re
import sys
import os
import argparse
from config import *

def get_prescribe_map():
    KCD_df = pd.read_csv(KCD_OUTPUT_PATH)
    KCD_to_code = pd.Series(KCD_df.mapping_code.values,index=KCD_df.KCD_code.values).to_dict()

    del KCD_df

    return KCD_to_code


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


def run(prescribe_data_path,date_type):
    chunks = pd.read_csv(prescribe_data_path, delimiter = DELIM, 
                header=None, names=COL_NAME, usecols=USE_COLS, chunksize=CHUNK_SIZE)
    # USE_COLS = ['no','KCD_code','date']
    
    KCD_to_code = get_prescribe_map() # mapping dictionary

    for idx, chunk in enumerate(chunks):
        #### mapping
        chunk.KCD_code = chunk.KCD_code.map(KCD_to_code)
        chunk.date = chunk.date.map(convert_date(date_type))
        
        if idx is 0:
            chunk.to_csv(PRESCRIBE_OUTPUT_PATH, sep=DELIM, header=USE_COLS,index=False)
        else:
            chunk.to_csv(PRESCRIBE_OUTPUT_PATH, sep=DELIM, header=False,index=False,mode='a')


def count():
    chunks = pd.read_csv(PRESCRIBE_OUTPUT_PATH, delimiter = DELIM, 
                header=None, names=USE_COLS, chunksize=CHUNK_SIZE)

    result = pd.concat([chunk.KCD_code.apply(pd.Series.value_counts) for chunk in chunks])
    result = result.groupby(result.index).sum()

    result.to_csv(KCD_COUNTS_PATH, sep=DELIM, index=False)

    print(result)


def set_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument("-i",help="prescribe data path")
    parser.add_argument("-d",help="the type of date : {0 : month, 1: quarter}")

    args = parser.parse_args()

    return args


if __name__=='__main__':
    args = set_parser() # param을 받아들임
    
    run(args.i,args.d)    