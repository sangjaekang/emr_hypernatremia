#-*- encoding :utf-8 -*-
import sys, os, re
# 상위 폴더의 config를 import하기 위한 경로 설정
os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)
from converter.config import *
from converter.convert_common import check_directory

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
    std_min = revise_avg(x)-revise_std(x)*3 # 3 시그마 바깥 값
    q_min = x.quantile(0.01)
    if std_min<0 :
        # 측정값중에서 음수가 없기 때문에, 음수인 경우는 고려안함
        return q_min
    else :
        return np.mean((std_min,q_min))

def revise_max(x):
    # 3시그마 바깥 값과 quanter 값의 사이값으로 결정
    std_max = revise_avg(x)+revise_std(x)*3
    q_max = x.quantile(0.99)
    return np.mean((std_max,q_max))


def change_number(x):
    '''
    숫자　표현을　통일
    （범위　쉼표　등　표현을　단일표현으로　통일）
    '''
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
    #labtest 데이터를　test별로　쪼개서　저장
    global DELIM, LAB_COL_NAME, CHUNK_SIZE, PER_LAB_DIR, USE_LAB_COL_NAME

    check_directory(PER_LAB_DIR)

    # remove temp file in per_lab_directory
    re_per_lab = re.compile("^labtest_.*\.csv") 
    for file in os.listdir(PER_LAB_DIR):
        if re_per_lab.match(file):
            os.remove(PER_LAB_DIR + file)

    chunks = pd.read_csv(lab_test_path, delimiter=DELIM, 
                                                header=None,names=LAB_COL_NAME ,chunksize=CHUNK_SIZE)

    for idx, chunk in enumerate(chunks):
        for lab_name in chunk.lab_code.unique():
            temp_save_df= chunk[chunk.lab_code.isin([lab_name])]
            save_path = PER_LAB_DIR + 'labtest_{}.csv'.format(lab_name)

            if os.path.isfile(save_path):
                # 파일이 존재한다면
                temp_save_df[USE_LAB_COL_NAME].to_csv(save_path,sep=DELIM, header=False, index=False, mode='a')
            else : 
                temp_save_df[USE_LAB_COL_NAME].to_csv(save_path,sep=DELIM, index=False)

            if DEBUG_PRINT:
                print('{}th {} completed'.format(idx,lab_name))


def get_mapping_table():
    '''
    labtest의 mapping table을　생성하는　함수
    평균/ 최솟값/최댓값으로　구성
    '''
    global DELIM, LAB_COL_NAME, CHUNK_SIZE, PER_LAB_DIR, LAB_MAPPING_PATH
    
    check_directory(MAPPING_DIR)
    check_directory(PER_LAB_DIR)
    
    output_path = MAPPING_DIR + LAB_MAPPING_PATH

    # if exists, remove output file
    if os.path.isfile(output_path):
        os.remove(output_path)

    # if per_labtest data not exists, raise error
    if not os.listdir(PER_LAB_DIR):
        raise ValueError("there is no data in per_lab_directory!")

    # get temp file in per_lab_directory
    re_per_lab = re.compile("^labtest_.*\.csv") 
    pattern_df = '{},{},{},{}\n'
    with open(output_path,'w') as f :        
        f.write(pattern_df.format(*MAP_LAB_COL_NAME))
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
                
                if DEBUG_PRINT:
                    print("write {} completed".format(per_lab_name))

def _set_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', help="lab_test path")
    args = parser.parse_args()

    return args


if __name__=='__main__':
    args = _set_parser()

    divide_per_test(args.i)
    get_mapping_table()