#-*- encoding:utf-8 -*-
import argparse
import os

INPUT_PATH  = None
OUTPUT_PATH = None
LINE_COUNTS = 100000
DELIM       = ','
SKIP        = False

def check_error_line(input_path): 
    f = open(input_path,encoding='cp949')
    first_line = f.readline()

    delim_count = first_line.count(',')

    count = 0
    error_code_list = []

    for line in f.readlines():
        _count = line.count(',')
        
        if _count is not delim_count:
            count = count +1
            error_code_list.append(line.split(',')[1])
        if count<10:
            
            print("error exists : " + line)
    print("-----------------------------------")
    print("total error line : {}".format(count))
    print("-----------------------------------")
    print("error code list : ")
    for code in pd.Series(error_code_list).unique():
        print(code)


def _check_size(file_path):
    if file_path is None:
        raise ValueError('no file path')

    f = open(file_path,mode='r')
    col_num = f.readline().count(DELIM)+1
    row_num = sum(1 for _ in f)

    print("dataframe의 (row,col) : ({},{})".
            format(row_num,col_num))


def _set_parser():
    global SKIP
    parser = argparse.ArgumentParser()

    parser.add_argument('-i', help="sampling data path")
    parser.add_argument('-o', help="output path")
    parser.add_argument('-l', help="line number")
    parser.add_argument('-skip', help="skip count dataframe")

    args = parser.parse_args()

    if args.skip:
        SKIP = True

    return args


def run(input_path,output_path,line_counts):
    global INPUT_PATH, OUTPUT_PATH, LINE_COUNTS

    if not input_path:
        raise ValueError("no input_path")
    else :
        INPUT_PATH = input_path
    if output_path:
        OUTPUT_PATH = output_path
    if line_counts:
        LINE_COUNTS = int(line_counts)

    if not SKIP:
        _check_size(INPUT_PATH) # 데이터 사이즈 확인

    if os.path.isfile(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)

    with open(INPUT_PATH,'r') as infile:
        with open(OUTPUT_PATH,'w') as outfile:
            for idx, line in enumerate(infile):
                if idx > LINE_COUNTS:
                    break
                outfile.write(line)


if __name__=='__main__':
    args = _set_parser() # argument reading

    run(args.i,args.o,args.l) # sampling
