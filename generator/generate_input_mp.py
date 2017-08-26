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


def write_metadata_README(path, label_name,time_length,gap_length,target_length,offset_detect_counts):
    metadata_README = '''
### parameter Setting
| parameter             | value |
| ---------------------    | ----- |
| label_name            | {}    |
| time_length           | {}    |
| gap_length            | {}    |
| target_length         | {}    |
| offset_detects_counts | {}    |
| Created date          | {}    |
'''.format(label_name,time_length,gap_length,target_length,offset_detect_counts,time.ctime())
    
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
    parser.add_argument('gap_period', help='gap_period')
    parser.add_argument('target_length',help='target_length')
    parser.add_argument('offset_detect_counts',help='target_length')
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    global PREP_OUTPUT_DIR, SAMPLE_PATIENT_PATH
    #argument
    args = _set_parser()
    label_name = args.label; label_list = list(range(1,int(args.label_range)))
    core_num = int(args.core_num); chunk_size = int(args.chunk_size)
    
    time_length = int(args.time_length); gap_period = int(args.gap_period)
    target_length = int(args.target_length); offset_detect_counts = int(offset_detect_counts)
    o_path = check_directory(args.path)

    PREP_OUTPUT_DIR = check_directory(PREP_OUTPUT_DIR)
    output_path = PREP_OUTPUT_DIR + SAMPLE_PATIENT_PATH

    write_metadata_README(o_path, label_name,time_length,gap_length,target_length,offset_detect_counts)

    sample_store = pd.HDFStore(output_path,mode='r')
    a = sample_store.select('data/{}'.format(label_name))
    sample_store.close()
    
    print("Creating pool with 7 workers")
    pool = multiprocessing.Pool(processes=core_num)
    print(pool._pool)
    print("Invoking apply(save_patient_input, no_range)")

    def save_input(no_range):
        return save_patient_input(no_range,label_name=label_name,
                                            input_path=o_path, time_length=time_length,
                                            gap_period=gap_period, target_length = target_length,
                                            offset_detect_counts=offset_detect_counts)

    counter = core_num

    for divider in range_divider(a,label_list,core_num,max_chunk_size):
        pool.apply_async(save_input,[divider])        
        counter=count-1
        if counter<=0: break

    pool.close()
    pool.join()
    
    print("Finished")