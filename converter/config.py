#-*- encoding :utf-8 -*-
import sys, re, os

# 상위 폴더의 config를 import하기 위한 경로 설정
os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
HOME_PATH = os_path[:find_path.search(os_path).span()[0]]
sys.path.append(HOME_PATH)

from emr_hypernatremia.config import *
