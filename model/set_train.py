# -*- coding: utf-8 -*-
import sys, os, re
import time
import random

os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)

from imputator.config import *
from imputator.impute_common import check_directory, get_timeseries_column, get_time_interval
from generator.generate_patient_emr import generate_emr
from imputator.impute_mean import get_np_array_emr

import multiprocessing
from multiprocessing import Pool,Queue,Lock,Array
import ctypes

# import module related to keras
import keras
from keras.utils import np_utils
from keras.utils import plot_model
from keras.callbacks import ModelCheckpoint

from sklearn.metrics import roc_auc_score
from sklearn.metrics import roc_curve


#QUEUE FLAG
FLAG_ALL_DONE = b'work_finished'
FLAG_WORKER_FINISHED_PROCESSING = b'worker_finished_processing'


def get_np(input_dir=None):
    output_list = [i for i in os.listdir(input_dir) if os.path.isdir(input_dir+i)]
    output_num = random.choice(output_list)
    file_name = random.choice(os.listdir(input_dir+'{}'.format(output_num)))
    file_path = input_dir +"{}/".format(output_num) +file_name
    return int(output_num), get_np_array_emr(file_path)


def train_generator_ml(dataset_size,n_calculation,
                                     arr_ft_1,arr_la_1,arr_ft_2,arr_la_2,
                                     input_dir=None):    
    turn_flag = 1
    while n_calculation>0:
    
        if turn_flag is 1 : 
            with arr_ft_1.get_lock():
                with arr_la_1.get_lock():
                    print("train get arr_ft_1 lock")
                    x_train, y_train = train_generator(dataset_size,input_dir=input_dir)
                    nparr_ft_1 = np.frombuffer(arr_ft_1.get_obj())
                    nparr_ft_1[:] = x_train.flatten()
                    nparr_la_1 = np.frombuffer(arr_la_1.get_obj())
                    nparr_la_1[:] = y_train.flatten()
                    turn_flag = 2
                    print("train unlock arr_ft_1")
        else :
            with arr_ft_2.get_lock():
                with arr_la_2.get_lock():
                    print("train get arr_ft_2 lock")
                    x_train, y_train = train_generator(dataset_size,input_dir=input_dir)
                    nparr_ft_2 = np.frombuffer(arr_ft_2.get_obj())
                    nparr_ft_2[:] = x_train.flatten()
                    nparr_la_2 = np.frombuffer(arr_la_2.get_obj())
                    nparr_la_2[:] = y_train.flatten()
                    turn_flag = 1
                    print("train unlock arr_ft_2")

        time.sleep(1)
        n_calculation = n_calculation -1

## make batch of train data set 
def train_generator(dataset_size,input_dir=None,core_num=6):
    global INPUT_DIR
    pool = Pool(processes=core_num)

    if input_dir is None:
        input_dir = INPUT_DIR
    else :
        input_dir = INPUT_DIR + input_dir
        input_dir = check_directory(input_dir)
    
    results = [pool.apply_async(get_np,(input_dir,)) for _ in range(dataset_size)]
    label_list = list(); stack_list = list()

    for result in results:
        a,b = result.get()
        label_list.append(a);stack_list.append(b)

    output_nums =len([i for i in os.listdir(input_dir) if os.path.isdir(input_dir+i)])
    batch_features = np.stack(stack_list,axis=0)
    batch_labels = np_utils.to_categorical(label_list,output_nums)

    return batch_features, batch_labels

        
def save_acc_loss_graph(acc_list,val_acc_list,file_path):
    fig = plt.figure(1)

    acc_list.extend(history.history['acc'])
    val_acc_list.extend(history.history['val_acc'])
    plt.subplot(121)
    plt.plot(acc_list)
    plt.plot(val_acc_list)
    plt.title('model accuracy')
    plt.ylabel('accuracy')
    plt.xlabel('epoch')
    plt.legend(['train','test'],loc='lower right')

    loss_list.extend(history.history['loss'])
    val_loss_list.extend(history.history['loss'])
    plt.subplot(122)
    plt.plot(loss_list)
    plt.plot(val_loss_list)
    plt.title('model loss')
    plt.ylabel('loss')
    plt.xlabel('epoch')
    plt.legend(['train','test'],loc='upper right')

    fig.set_size_inches(12., 6.0)

    fig.savefig(file_path+'acc_and_loss graph.png',dpi=100)


def fit_train(model,dataset_size,o_path,validation_split=0.33,batch_size=256,epochs=200):
    global MODEL_SAVE_DIR,DEBUG_PRINT
    file_path = MODEL_SAVE_DIR + o_path
    check_directory(file_path)
    file_name = file_path+'weights-improvement-{epoch:02d}-{val_acc:.2f}.hdf5'
    
    # save the model_parameter
    checkpoint = ModelCheckpoint(file_name, monitor='val_acc', verbose=0, save_best_only=True, mode='max')
    callbacks_list = [checkpoint]

    acc_list = []; val_acc_list = []
    loss_list = []; val_loss_list = []

    start_time = time.time()
    if DEBUG_PRINT:
        print("start!")
    x_train,y_train = train_generator(dataset_size)
    if DEBUG_PRINT:
        print("time consumed -- {}".format(time.time()-start_time))
    start_time = time.time()
    if DEBUG_PRINT:
        print("make batch clear")
    history = model.fit(x_train,y_train,
                        validation_split=validation_split, batch_size=batch_size,
                        epochs=epochs,callbacks=callbacks_list,verbose=0)
    if DEBUG_PRINT:                   
        print("end -- {}".format(time.time()-start_time))
    acc_list.extend(history.history['acc'])
    val_acc_list.extend(history.history['val_acc'])
    loss_list.extend(history.history['loss'])
    val_loss_list.extend(history.history['loss'])

    #save the accuracy and loss graph
    save_acc_loss_graph(acc_list,val_acc_list,file_path)


def fit_train_ml(model,data_shape,
                 arr_ft_1,arr_la_1,arr_ft_2,arr_la_2,
                 n_calculation, o_path, validation_split=0.33,batch_size=256,epochs=200):

    dataset_size,n_rows,n_cols,n_depths,n_labels = data_shape

    turn_flag = 1

    global MODEL_SAVE_DIR,DEBUG_PRINT
    file_path = MODEL_SAVE_DIR + o_path
    check_directory(file_path)
    file_name = file_path+'weights-improvement-{epoch:02d}-{val_acc:.2f}.hdf5'
    
    # save the model_parameter
    checkpoint = ModelCheckpoint(file_name, monitor='val_acc', verbose=0, save_best_only=True, mode='max')
    callbacks_list = [checkpoint]

    acc_list = []; val_acc_list = []
    loss_list = []; val_loss_list = []

    while n_calculation > 0:
        if turn_flag is 1:
            with arr_ft_1.get_lock():
                with arr_la_1.get_lock():
                    start_time = time.time()
                    if DEBUG_PRINT: print("fit_train_ml start!")
                    
                    nparr_ft_1 = np.frombuffer(arr_ft_1.get_obj())
                    nparr_ft_1 = np.reshape(nparr_ft_1,(dataset_size,n_rows,n_cols,n_depths)) 
                    nparr_la_1 = np.frombuffer(arr_la_1.get_obj())
                    nparr_la_1 = np.reshape(nparr_la_1,(dataset_size,n_labels))
                        
                    history = model.fit(nparr_ft_1,nparr_la_1,
                                        validation_split=validation_split, batch_size=batch_size,
                                        epochs=epochs,callbacks=callbacks_list,verbose=0)
                    
                    if DEBUG_PRINT: print("fit_train_ml end -- {}".format(time.time()-start_time))

                    acc_list.extend(history.history['acc'])
                    val_acc_list.extend(history.history['val_acc'])
                    loss_list.extend(history.history['loss'])
                    val_loss_list.extend(history.history['loss'])
                    turn_flag = 2
        else :
            with arr_ft_2.get_lock():
                with arr_la_2.get_lock():
                    start_time = time.time()
                    if DEBUG_PRINT: print("fit_train_ml start!")
                    
                    nparr_ft_2 = np.frombuffer(arr_ft_2.get_obj())
                    nparr_ft_2 = np.reshape(nparr_ft_2,(dataset_size,n_rows,n_cols,n_depths)) 
                    nparr_la_2 = np.frombuffer(arr_la_2.get_obj())
                    nparr_la_2 = np.reshape(nparr_la_2,(dataset_size,n_labels))
                        
                    history = model.fit(nparr_ft_2,nparr_la_2,
                                        validation_split=validation_split, batch_size=batch_size,
                                        epochs=epochs,callbacks=callbacks_list,verbose=0)
                    
                    if DEBUG_PRINT: print("fit_train_ml end -- {}".format(time.time()-start_time))

                    acc_list.extend(history.history['acc'])
                    val_acc_list.extend(history.history['val_acc'])
                    loss_list.extend(history.history['loss'])
                    val_loss_list.extend(history.history['loss'])
                    turn_flag = 1
        time.sleep(1)
        n_calculation = n_calculation-1

    #save the accuracy and loss graph
    save_acc_loss_graph(acc_list,val_acc_list,file_path)