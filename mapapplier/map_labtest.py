#-*- encoding :utf-8 -*-
from config import *
from map_common import convert_date

def normalize_number(avg_x,min_x,max_x,x):
    str_x = str(x).strip()

    re_num = re.compile('^[+-]?[\d]+[.]?[\d]*$')
    re_lower = re.compile('^<[\d\s]*[.]{0,1}[\d\s]*$')
    re_upper = re.compile('^>[\d\s]*[.]{0,1}[\d\s]*$')
    re_star = re.compile('^[\s]*[*][\s]*$')
    if re_num.match(str_x):
        float_x = float(str_x)
        return (float_x-min_x)/(max_x-min_x)
    else:
        if re_lower.match(str_x):
            return 0
        elif re_upper.match(str_x):
            return 1
        elif re_star.match(str_x):
            return (avg_x-min_x)/(max_x-min_x)
        else:
            return np.nan


def get_labtest_value(df,labtest_name):
    _temp = df[df.labtest.isin([labtest_name])]
    return _temp.avg.values[0], _temp.min.values[0], _temp.max.values[0]


def get_labtest_map():
    global LABMAP_OUTPUT_PATH, DELIM

    if not os.path.isfile(LABMAP_OUTPUT_PATH):
        raise ValueError("There is no labtest_OUTPUT file!")

    labtest_mapping_df = pd.read_csv(LABMAP_OUTPUT_PATH,delimiter=DELIM)

    return labtest_mapping_df


def run(labtest_data_path,date_type):
    global LABTEST_OUTPUT_PATH, DEBUG_PRINT

    if os.path.isfile(LABTEST_OUTPUT_PATH):
        os.remove(LABTEST_OUTPUT_PATH)

    labtest_mapping_df = get_labtest_map()

    re_p = re.compile('^[^,\n]*,[^,\n]*,[^,\n]*,[^,\n]*\n$')
    output_pattern = '{},{},{},{}\n'

    with open(labtest_data_path,'r') as inputfile:
        with open(LABTEST_OUTPUT_PATH,'w') as outputfile:
            
            outputfile.write(output_pattern.format('no','lab_code','date','result'))
            
            for idx, input_line in enumerate(inputfile.readlines()):
                if re_p.match(input_line):
                    no, lab_code, date, result = input_line.strip().split(',')
                    
                    r_avg, r_min, r_max = get_labtest_value(labtest_mapping_df,lab_code)    
                    revised_result = normalize_number(r_avg,r_min,r_max,result)
                    revised_date = convert_date(date_type)(date)

                    output_str = output_pattern.format(no, lab_code, revised_date, revised_result)
                    
                    outputfile.write(output_str)

                    if DEBUG_PRINT and (idx % 1000000 == 0) :
                        print('---{}th line start---'.format(idx))

                else : 
                    
                    if DEBUG_PRINT:
                        print("EXCEPTION CASE in labtest_data  ---> {}".format(input_line))
                    
                    pass


def set_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument("-i",help="labtest data path")
    parser.add_argument("-d",help="the type of date : {0 : month, 1: quarter}")

    args = parser.parse_args()

    return args


if __name__=='__main__':
    args = set_parser()
    
    run(args.i,args.d)    