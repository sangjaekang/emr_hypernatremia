#-*- coding: utf-8 -*-
import sys, os, re

os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)
from generator.config import *
from generator.construct_common import check_directory ,get_time_interval
from generator.generate_patient_emr import generate_emr, get_patient_timeseries_label, save_patient_input

from imputator.impute_mean import *
import h5py
import time
import multiprocessing


def range_divider(a,label_list,core_num,max_chunk_size):
    used_no = set()
    counter = core_num

    per_counter = np.int(np.ceil(np.float(core_num)/np.float(len(label_list))))
    while True:
        label=label_list.pop()
        for _ in range(per_counter):
            no_range = list(set(a[a.label==label].no.unique())-used_no)[:max_chunk_size]
            used_no = set(no_range) | used_no
            yield no_range


def write_metadata_README(path, label_name,time_length,gap_length,target_length,offset_min_counts,offset_max_counts):
    metadata_README = '''
### parameter Setting
| parameter             | value |
| ---------------------    | ----- |
| label_name            | {}    |
| time_length           | {}    |
| gap_length            | {}    |
| target_length         | {}    |
| offset_min_counts | {}    |
| offset_max_counts | {}    |
| Created date          | {}    |
'''.format(label_name,time_length,gap_length,target_length,offset_min_counts,offset_max_counts,time.ctime())
    
    file_path = path + 'README.md'    
    with open(file_path,'w') as f:
        f.write(metadata_README)


def _set_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('path', help='save path')
    parser.add_argument('label', help='label_name')
    parser.add_argument('label_range',help='label range')
    parser.add_argument('core_num',help='using core num' )
    parser.add_argument('chunk_size',help='working chunk size')

    parser.add_argument('time_length',help='time_length')
    parser.add_argument('gap_length', help='gap_length')
    parser.add_argument('target_length',help='target_length')
    parser.add_argument('offset_min_counts',help='offset_min_counts')
    parser.add_argument('offset_max_counts',help='offset_max_counts')
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    global PREP_OUTPUT_DIR, SAMPLE_PATIENT_PATH
    #argument
    args = _set_parser()
    label_name = args.label 
    label_list = list(range(1,int(args.label_range)))
    core_num = int(args.core_num)
    chunk_size = int(args.chunk_size)
    time_length = int(args.time_length) 
    gap_length = int(args.gap_length)
    target_length = int(args.target_length)
    offset_min_counts = int(args.offset_min_counts)
    offset_max_counts = int(args.offset_max_counts) 
    
    o_path = check_directory(args.path)

    train_path = o_path + 'train/'; train_path = check_directory(train_path)
    test_path = o_path + 'test/'; test_path = check_directory(test_path)
    validation_path = o_path + 'validation/'; validation_path = check_directory(validation_path)

    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)
    output_path = PREP_OUTPUT_DIR + SAMPLE_PATIENT_PATH

    write_metadata_README(o_path, label_name,time_length,gap_length,target_length,offset_min_counts,offset_max_counts)

    sample_store = pd.HDFStore(output_path,mode='r')
    train_set = sample_store.select('data/{}/train'.format(label_name))
    validation_set = sample_store.select('data/{}/validation'.format(label_name))
    test_set = sample_store.select('data/{}/test'.format(label_name))
    sample_store.close()

    ## train set generating
    print("Creating pool with 7 workers")
    pool = multiprocessing.Pool(processes=core_num)
    print(pool._pool)
    print("Invoking apply train_set")
    counter = core_num
    for divider in range_divider(train_set,label_list,core_num,chunk_size):
        pool.apply_async(save_patient_input,[divider,label_name,train_path,time_length,
                         gap_length,target_length,offset_min_counts,offset_max_counts])
        counter=counter-1
        if counter<=0: break
    pool.close()
    pool.join()    
    print("trainset Finished")

    ## test set generating
    print("Creating pool with 7 workers")
    pool = multiprocessing.Pool(processes=core_num)
    print(pool._pool)
    print("Invoking apply test_set")
    counter = core_num
    for divider in range_divider(test_set,label_list,core_num,chunk_size):
        pool.apply_async(save_patient_input,[divider,label_name,test_path,time_length,
                         gap_length,target_length,offset_min_counts,offset_max_counts])
        counter=counter-1
        if counter<=0: break
    pool.close()
    pool.join()    
    print("test_set Finished")

    ## train set generating
    print("Creating pool with 7 workers")
    pool = multiprocessing.Pool(processes=core_num)
    print(pool._pool)
    print("Invoking apply validation_set")
    counter = core_num
    for divider in range_divider(validation_set,label_list,core_num,chunk_size):
        pool.apply_async(save_patient_input,[divider,label_name,validation_path,time_length,
                         gap_length,target_length,offset_min_counts,offset_max_counts])
        counter=counter-1
        if counter<=0: break
    pool.close()
    pool.join()    
    print("validation_set Finished")