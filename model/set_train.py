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
from multiprocessing import Pool,Queue,Lock

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
    global INPUT_DIR
    if input_dir is None:
        input_dir = INPUT_DIR
    else :
        input_dir = INPUT_DIR + input_dir
        input_dir = check_directory(input_dir)
    
    output_list = [i for i in os.listdir(input_dir) if os.path.isdir(input_dir+i)]
    output_num = random.choice(input_dir + output_list)
    file_name = random.choice(os.listdir(input_dir+'{}'.format(output_num)))
    file_path = input_dir +"{}/".format(output_num) +file_name
    return int(output_num), get_np_array_emr(file_path)


## make batch of train data set 
def train_generator(dataset_size,input_dir=None,core_num=6,queue_count=1,queue=None):
    pool = Pool(processes=core_num)
    global INPUT_DIR
    if input_dir is None:
        input_dir = INPUT_DIR
    else :
        input_dir = INPUT_DIR + input_dir
        input_dir = check_directory(input_dir)
    
    while queue_count >=1:
    # queue_count : the counts of loop for inserting batch into queue
        if DEBUG_PRINT:
            print("pool.apply_async start!")
        results = [pool.apply_async(get_np,(input_dir,)) for _ in range(dataset_size)]
        label_list = list(); stack_list = list()
        
        for result in results:
            a,b = result.get()
            label_list.append(a);stack_list.append(b)

        output_nums =len([i for i in os.listdir(input_dir) if os.path.isdir(input_dir+i)])
        batch_features = np.stack(stack_list,axis=0)
        batch_labels = np_utils.to_categorical(label_list,output_nums)

        del label_list, stack_list, results
        queue_count = queue_count-1
        
        if queue is None:
            return batch_features, batch_labels
        else :
            if DEBUG_PRINT:
                print("{}counts left".format(queue_count))
            queue.put((batch_features,batch_label))

    # empty queue setting
    queue.put(FLAG_WORKER_FINISHED_PROCESSING)


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


def fit_train(model,dataset_size,o_path,validation_split=0.33,batch_size=256,epochs=200,queue=None):
    global MODEL_SAVE_DIR,DEBUG_PRINT
    file_path = MODEL_SAVE_DIR + o_path
    check_directory(file_path)
    file_name = file_path+'weights-improvement-{epoch:02d}-{val_acc:.2f}.hdf5'
    
    # save the model_parameter
    checkpoint = ModelCheckpoint(filepath, monitor='val_acc', verbose=0, save_best_only=True, mode='max')
    callbacks_list = [checkpoint]

    acc_list = []; val_acc_list = []
    loss_list = []; val_loss_list = []

    if queue is None:
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
    else:
        while True:
            start_time = time.time()
            q_result = queue.get()
            if DEBUG_PRINT:
                print("time consumed -- {}".format(time.time()-start_time))
            start_time = time.time()
            if new_result == FLAG_WORKER_FINISHED_PROCESSING:
                break
            else:
                x_train,y_train = q_result
                if DEBUG_PRINT:
                    print("make batch clear")
                history = model.fit(x_train,y_train,
                    validation_split=0.33, batch_size=batch_size,
                    epochs=epochs,callbacks=callbacks_list,verbose=0)
                if DEBUG_PRINT:
                    print("end -- {}".format(time.time()-start_time))
                acc_list.extend(history.history['acc'])
                val_acc_list.extend(history.history['val_acc'])
                loss_list.extend(history.history['loss'])
                val_loss_list.extend(history.history['loss'])
    
    #save the accuracy and loss graph
    save_acc_loss_graph(acc_list,val_acc_list,file_path)
