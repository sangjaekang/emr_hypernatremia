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