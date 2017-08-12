#-*- encoding :utf-8 -*-

import pandas as pd
import numpy as np
import re
import sys
import os
from config import *
import argparse

def set_col_type(col_type):
    
    if type(col_type) is str: 
        col_type = int(col_type)
    
    if col_type is 0:
        return "대분류명"
    elif col_type is 1:
        return '중분류명'
    elif col_type is 2:
        return '소분류명'
    else :
        return '중분류명'


def preprocess_middle_class(df):
    df['중분류명'] = df['중분류명'].str.strip().replace('A92-A99',"A90-A99")
    df['중분류명'] = df['중분류명'].str.strip().replace('G10-G13',"G10-G14")    
    df['중분류명'] = df['중분류명'].str.strip().replace('K55-K63',"K55-K63")

    return df


def get_mapping_table(df,col_type):
    code_to_class = pd.Series(df[col_type].values, index=df['진단용어코드'])
    class_to_int = pd.Series(range(1,len(df[col_type].unique())+1),index=df[col_type].unique())
    
    mapping_df = pd.concat([code_to_class,code_to_class.map(class_to_int)],
                axis=1,keys=code_to_class)

    mapping_df.index.name = 'KCD_code'
    mapping_df.columns = ['class_code','mapping_code']

    mapping_df.to_csv(KCD_OUTPUT_PATH,sep=DELIM)

    del mapping_df


def run(col_type):
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
    global SKIP
    parser = argparse.ArgumentParser()

    parser.add_argument('-t', help="select col_type (0 : 대분류명, 1 : 중분류명, 2 : 소분류명")

    args = parser.parse_args()

    return args


if __name__=='__main__':
    args = _set_parser()

    run(args.t)