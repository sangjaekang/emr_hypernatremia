# -*- coding: utf-8 -*-
import sys, re, os


os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
HOME_PATH = os_path[:find_path.search(os_path).span()[0]]
sys.path.append(HOME_PATH)

from emr_hypernatremia.config import *

import h5py

CHUNK_SIZE = 10000000