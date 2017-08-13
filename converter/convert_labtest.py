#-*- encoding :utf-8 -*-
import pandas as pd
import numpy as np
import re
import sys
import os
from config import *
import argparse


def check_df_size(lab_test_path):
    # 우리가 읽을 dataframe의 사이즈를 체크
    global  CHUNK_SIZE

    if lab_test_path is None:
        raise ValueError("no lab test data path")

    f = open(lab_test_path,mode='r',encoding="utf-8")
    col_num = f.readline().count(DELIM)+1
    row_num = sum(1 for _ in f)

    print("dataframe의 (row,col) : ({},{}) ".
            format(row_num,col_num))    


def revise_avg(x):
    # 10~90% 내에 있는 값을 이용해서 평균 계산
    quan_min = x.quantile(0.10)
    quan_max = x.quantile(0.90)
    return x[(x>quan_min) & (x<quan_max)].mean()

def revise_std(x):
    # 1~99% 내에 있는 값을 이용해서 표준편차 계산
    quan_min = x.quantile(0.01)
    quan_max = x.quantile(0.99)
    return x[(x>quan_min) & (x<quan_max)].std()

def revise_min(x):
    # 3시그마 바깥 값과 quanter 값의 사이값으로 결정
    std_min = revise_mean(x)-revise_std(x)*3 # 3 시그마 바깥 값
    q_min = x.quantile(0.01)
    if std_min<0 :
        # 측정값중에서 음수가 없기 때문에, 음수인 경우는 고려안함
        return q_min
    else :
        return np.mean((std_min,q_min))

def revise_max(x):
    # 3시그마 바깥 값과 quanter 값의 사이값으로 결정
    std_max = revise_mean(x)+revise_std(x)*3
    q_max = x.quantile(0.99)
    return np.mean((std_max,q_max))


def change_number(x):
    str_x = str(x).replace(" ","")

    re_num   = re.compile('^[+-]{0,1}[\d\s]+[.]{0,1}[\d\s]*$') #숫자로 구성된 데이터를 float로 바꾸어 줌
    re_comma = re.compile('^[\d\s]*,[\d\s]*[.]{0,1}[\d\s]*$') # 쉼표(,)가 있는 숫자를 선별
    re_range = re.compile('^[\d\s]*[~\-][\d\s]*$') # 범위(~,-)가 있는 숫자를 선별

    if re_num.match(str_x):
        return float(str_x)
    else:
        if re_comma.match(str_x):
            return change_number(str_x.replace(',',""))

        elif re_range.match(str_x):
            if "~" in str_x:
                a,b = str_x.split("~")
            else:
                a,b = str_x.split("-")

            return np.mean((change_number(a),change_number(b)))
        else :
            return np.nan


def divide_per_test(lab_test_path):    
    global DELIM, COL_NAME, CHUNK_SIZE, PER_LAB_DIR

    # syntax checking for directory
    if not (PER_LAB_DIR[-1] is '/'):
        PER_LAB_DIR  = PER_LAB_DIR + '/'

    # not exists in directory
    if not os.path.exists(PER_LAB_DIR):
        os.makedirs(PER_LAB_DIR)

    # remove temp file in per_lab_directory
    re_per_lab = re.compile("^labtest_.*\.csv") 
    for file in os.listdir(PER_LAB_DIR):
        if re_per_lab.match(file):
            os.remove(PER_LAB_DIR + file)

    chunks = pd.read_csv(lab_test_path, delimiter=DELIM, 
                                                header=None,names=COL_NAME ,chunksize=CHUNK_SIZE)

    for idx, chunk in enumerate(chunks):
        print("{} - start".format(idx))
        for lab_name in chunk.lab_code.unique():
            temp_save_df= chunk[chunk.lab_code.isin([lab_name])]
            save_path = PER_LAB_DIR + 'labtest_{}.csv'.format(lab_name)
            if os.path.isfile(save_path):
                # 파일이 존재한다면
                temp_save_df[['no','date','result']].to_csv(save_path,sep=DELIM, header=False, index=False, mode='a')
            else : 
                temp_save_df[['no','date','result']].to_csv(save_path,sep=DELIM, index=False)


def get_mapping_table():
    global DELIM, COL_NAME, CHUNK_SIZE, PER_LAB_DIR, LAB_OUTPUT_PATH
    
    # if exists, remove output file
    if os.path.isfile(LAB_OUTPUT_PATH):
        os.remove(LAB_OUTPUT_PATH)

    # syntax checking for directory
    if not (PER_LAB_DIR[-1] is '/'):
        PER_LAB_DIR  = PER_LAB_DIR + '/'

    # not exists in directory
    if not os.path.exists(PER_LAB_DIR):
        os.makedirs(PER_LAB_DIR)

    # get temp file in per_lab_directory
    re_per_lab = re.compile("^labtest_.*\.csv") 
    
    pattern_df = '{},{},{},{}\n'
    with open(LAB_OUTPUT_PATH,'w') as f :        
        f.write(pattern_df.format('labtest','avg','min','max'))
        for  file in os.listdir(PER_LAB_DIR):
            if re_per_lab.match(file):
                per_lab_name = file.replace('labtest_','').replace('.csv','')
                per_lab_path = PER_LAB_DIR + file

                per_lab_df = pd.read_csv(per_lab_path,delimiter=DELIM)

                # 1. 숫자로　치환하기
                per_lab_df.result = per_lab_df.result.map(change_number)
                # 2. 이상 값 처리 시 대응되는 값
                r_avg   = revise_avg(per_lab_df.result)
                r_min  = revise_min(per_lab_df.result)
                r_max = revise_max(per_lab_df.result)
                # 3. save
                f.write(pattern_df.format(per_lab_name, r_avg, r_min, r_max))


def _set_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument('-i', help="lab_test path")

    args = parser.parse_args()

    return args


if __name__=='__main__':
    args = _set_parser()

    divide_per_test(args.i)
