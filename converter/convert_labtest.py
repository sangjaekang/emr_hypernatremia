#-*- encoding :utf-8 -*-
import pandas as pd
import numpy as np
import re
import sys
import os
from config import *
import argparse

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
                temp_save_df.to_csv(save_path,sep=DELIM, header=False, index=False, mode='a')
            else : 
                temp_save_df.to_csv(save_path,sep=DELIM, index=False)


if __name__=='__main__':
    args = _set_parser()
