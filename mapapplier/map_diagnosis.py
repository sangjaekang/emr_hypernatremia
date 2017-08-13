#-*- encoding :utf-8 -*-
import pandas as pd
import numpy as np
import re
import sys
import os
import argparse
from config import *

def get_diagnosis_map():
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


def run(diagnosis_data_path,date_type):
    chunks = pd.read_csv(diagnosis_data_path, delimiter = DELIM, 
                header=None, names=COL_NAME, usecols=USE_COLS, chunksize=CHUNK_SIZE)
    # USE_COLS = ['no','KCD_code','date']
    
    KCD_to_code = get_diagnosis_map() # mapping dictionary

    for idx, chunk in enumerate(chunks):
        #### mapping
        chunk.KCD_code = chunk.KCD_code.map(KCD_to_code)
        chunk.date = chunk.date.map(convert_date(date_type))
        
        if idx is 0:
            chunk.to_csv(DIAGNOSIS_OUTPUT_PATH, sep=DELIM, header=USE_COLS,index=False)
        else:
            chunk.to_csv(DIAGNOSIS_OUTPUT_PATH, sep=DELIM, header=False,index=False,mode='a')


def count():
    chunks = pd.read_csv(DIAGNOSIS_OUTPUT_PATH, delimiter = DELIM, chunksize=CHUNK_SIZE)

    result = pd.concat([pd.value_counts(chunk.KCD_code.values,sort=False) for chunk in chunks])
    result = result.groupby(result.index).sum()

    result.to_csv(KCD_COUNTS_PATH, sep=DELIM, index=True)


def drop_useless_data():
    KCD_count_df = pd.read_csv(KCD_COUNTS_PATH,names=['mapping_code','count'], delimiter=DELIM)
    KCD_mapping_df = pd.read_csv(KCD_OUTPUT_PATH,delimiter=DELIM)

    int_to_code = pd.Series(KCD_mapping_df.class_code.unique(),index = KCD_mapping_df.mapping_code.unique()).to_dict()

    KCD_count_df['KCD_code'] = KCD_count_df['mapping_code'].map(int_to_code)

    cutoff_count = KCD_count_df['count'].sum() // DROP_RATE
    use_df  = KCD_count_df[KCD_count_df['count']>=cutoff_count]

    KCD_mapping_df = KCD_mapping_df[KCD_mapping_df.mapping_code.isin(use_df.mapping_code)]
    KCD_mapping_df.to_csv(KCD_OUTPUT_PATH,sep=DELIM)
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


def set_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument("-i",help="diagnosis data path")
    parser.add_argument("-d",help="the type of date : {0 : month, 1: quarter}")

    args = parser.parse_args()

    return args


if __name__=='__main__':
    args = set_parser()
    
    run(args.i,args.d)    