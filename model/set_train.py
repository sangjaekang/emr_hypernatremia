# -*- coding: utf-8 -*-
import sys, os, re
import time
import random
import argparse

os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)

from imputator.config import *
from imputator.impute_common import check_directory, get_timeseries_column, get_time_interval
from generator.generate_patient_emr import generate_emr
from imputator.impute_mean import get_np_array_emr

import multiprocessing
from multiprocessing import Pool,Queue,Lock,Array,Process
import ctypes

from model import prediction_model

# import module related to keras
import keras
from keras.utils import np_utils
from keras.utils import plot_model
from keras.callbacks import ModelCheckpoint

from sklearn.metrics import roc_auc_score
from sklearn.metrics import roc_curve
import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt

#QUEUE FLAG
FLAG_ALL_DONE = b'work_finished'
FLAG_WORKER_FINISHED_PROCESSING = b'worker_finished_processing'


def get_testset(input_dir=None,testset_size=1200):
    # 데이터셋에서　testset을　일부　분리시키는　작업
    testset_dir = input_dir + '/testset/'
    
    if os.path.isdir(testset_dir): # check the existence of testset
        return
    re_num = re.compile("^\d+$")
    print("Setting testset...directory : {}".format(testset_dir))
    output_list = [i for i in os.listdir(input_dir) if os.path.isdir(input_dir+i) and re_num.match(i)]
    num_cases = len(output_list)
    for i in output_list:
        if not os.path.isdir(testset_dir + str(i) + '/'):
            os.makedirs(testset_dir+str(i)+'/')
        for _ in range(np.int(testset_size/num_cases)):
            file_name = random.choice(os.listdir(input_dir+str(i)))
            src_path = input_dir +'{}/{}'.format(i,file_name)
            dst_path = testset_dir + '{}/{}'.format(i,file_name)
            os.rename(src_path,dst_path)


def eval_trainset(model, model_path, testset_path):
    re_hdf = re.compile(".*\.hdf5$")
    acc_df = pd.DataFrame(columns=['filename','acc'])
    model_path = MODEL_SAVE_DIR+model_path
    model_path = check_directory(model_path)
    
    for file_name in os.listdir(model_path):
        if re_hdf.match(file_name):
            acc_df = acc_df.append({'acc':file_name.split('-')[-1].replace(".hdf5",""),'filename':file_name},
                       ignore_index=True)
    best_acc_file = acc_df.sort_values(['acc'],ascending=False).iloc[0].filename
    
    model.load_weights(model_path+'/'+best_acc_file)

    X,Y = train_generator(600, testset_path,shuffling=False,test=True)
    Y_pred = model.predict(X)
    score = model.evaluate(X,Y, verbose=0)
    print("Evaluate testset!")
    print("--------------------------------------------------------------")
    print("%s: %.4f%%" % (model.metrics_names[1], score[1]*100))
    print("roc_auc_score : {}".format(roc_auc_score(Y,Y_pred)))
    print("--------------------------------------------------------------")


def get_np(input_dir=None,shuffling=True):
    re_num = re.compile("^\d+$")
    output_list = [i for i in os.listdir(input_dir) if os.path.isdir(input_dir+i) and re_num.match(i)]
    output_num = random.choice(output_list)
    file_name = random.choice(os.listdir(input_dir+'{}'.format(output_num)))
    file_path = input_dir +"{}/".format(output_num) +file_name
    return int(output_num), get_np_array_emr(file_path,shuffling=True)


def train_generator_ml(dataset_size,n_calculation,
                                     arr_ft_1,arr_la_1,arr_ft_2,arr_la_2,
                                     input_dir=None):    
    global INPUT_DIR
    _dir = INPUT_DIR + input_dir
    if os.path.isdir(_dir):
        get_testset(_dir,1200)
    else : 
        raise ValueError("wrong Input directory!")

    turn_flag = 1
    while n_calculation>0:
    
        if turn_flag is 1 : 
            with arr_ft_1.get_lock():
                with arr_la_1.get_lock():
                    start_time = time.time()
                    print("train get arr_ft_1 lock")
                    x_train, y_train = train_generator(dataset_size,input_dir=input_dir)
                    nparr_ft_1 = np.frombuffer(arr_ft_1.get_obj())
                    nparr_ft_1[:] = x_train.flatten()
                    nparr_la_1 = np.frombuffer(arr_la_1.get_obj())
                    nparr_la_1[:] = y_train.flatten()
                    turn_flag = 2
                    print("train unlock arr_ft_1 --- time consumed : {}".format(time.time()-start_time))
        else :
            with arr_ft_2.get_lock():
                with arr_la_2.get_lock():
                    start_time = time.time()
                    print("train get arr_ft_2 lock")
                    x_train, y_train = train_generator(dataset_size,input_dir=input_dir)
                    nparr_ft_2 = np.frombuffer(arr_ft_2.get_obj())
                    nparr_ft_2[:] = x_train.flatten()
                    nparr_la_2 = np.frombuffer(arr_la_2.get_obj())
                    nparr_la_2[:] = y_train.flatten()
                    turn_flag = 1
                    print("train unlock arr_ft_2 --- time consumed : {}".format(time.time()-start_time))

        time.sleep(1)
        n_calculation = n_calculation -1

## make batch of train data set 
def train_generator(dataset_size,input_dir=None,core_num=6,shuffling=True,test=False):
    global INPUT_DIR
    pool = Pool(processes=core_num)

    if input_dir is None:
        input_dir = INPUT_DIR
    else :
        input_dir = INPUT_DIR + input_dir
        input_dir = check_directory(input_dir)
    
    if not test:
        get_testset(input_dir,1200)

    results = [pool.apply_async(get_np,(input_dir,shuffling,)) for _ in range(dataset_size)]
    
    label_list = list(); stack_list = list()

    for result in results:
        a,b = result.get()
        label_list.append(a)
        stack_list.append(b)

    pool.close()
    pool.join()
    re_num = re.compile("^\d+$")
    output_nums =len([i for i in os.listdir(input_dir) if os.path.isdir(input_dir+i) and re_num.match(i) ])
    batch_features = np.stack(stack_list,axis=0)
    batch_labels = np_utils.to_categorical(label_list,output_nums)

    return batch_features, batch_labels


def save_acc_loss_graph(acc_list,val_acc_list,loss_list,val_loss_list,file_path):
    fig = plt.figure(1)

    plt.subplot(121)
    plt.plot(acc_list)
    plt.plot(val_acc_list)
    plt.title('model accuracy')
    plt.ylabel('accuracy')
    plt.xlabel('epoch')
    plt.legend(['train','test'],loc='lower right')

    plt.subplot(122)
    plt.plot(loss_list)
    plt.plot(val_loss_list)
    plt.title('model loss')
    plt.ylabel('loss')
    plt.xlabel('epoch')
    plt.legend(['train','test'],loc='upper right')

    fig.set_size_inches(12., 6.0)

    fig.savefig(file_path+'acc_and_loss graph.png',dpi=100)
    #plt.show()
    del fig


def fit_train(model,dataset_size,o_path,input_dir=None,validation_split=0.33,batch_size=256,epochs=200):
    global MODEL_SAVE_DIR,DEBUG_PRINT
    file_path = MODEL_SAVE_DIR + o_path
    check_directory(file_path)
    file_name = file_path+'weights-improvement-{epoch:03d}-{val_acc:.3f}.hdf5'
    
    # save the model_parameter
    checkpoint = ModelCheckpoint(file_name, monitor='val_acc', verbose=0, save_best_only=True, mode='max')
    callbacks_list = [checkpoint]

    acc_list = []; val_acc_list = []
    loss_list = []; val_loss_list = []

    start_time = time.time()
    if DEBUG_PRINT:
        print("start!")
    x_train,y_train = train_generator(dataset_size,input_dir)
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
    save_acc_loss_graph(acc_list,val_acc_list,loss_list,val_loss_list,file_path)
    # save the model
    plot_model(model,to_file=file_path+'model.png',show_shapes=True)

def fit_train_ml(model,data_shape,
                 arr_ft_1,arr_la_1,arr_ft_2,arr_la_2,
                 n_calculation, o_path, validation_split=0.5,batch_size=256,epochs=200):

    dataset_size,n_rows,n_cols,n_depths,n_labels = data_shape

    turn_flag = 1

    global MODEL_SAVE_DIR,DEBUG_PRINT
    model_path = MODEL_SAVE_DIR + o_path
    model_path= check_directory(model_path)
    file_name = model_path+'weights-improvement-{epoch:03d}-{val_acc:.3f}.hdf5'
    
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
                    sys.stdout.flush() 
                    nparr_ft_1 = np.frombuffer(arr_ft_1.get_obj())
                    nparr_ft_1 = np.reshape(nparr_ft_1,(dataset_size,n_rows,n_cols,n_depths)) 
                    nparr_la_1 = np.frombuffer(arr_la_1.get_obj())
                    nparr_la_1 = np.reshape(nparr_la_1,(dataset_size,n_labels))
                        
                    history = model.fit(nparr_ft_1,nparr_la_1,
                                        validation_split=validation_split, batch_size=batch_size,
                                        epochs=epochs,callbacks=callbacks_list,verbose=0)
                    
                    if DEBUG_PRINT: print("fit_train_ml end -- {}".format(time.time()-start_time))
                    sys.stdout.flush()
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
                    sys.stdout.flush()
                    nparr_ft_2 = np.frombuffer(arr_ft_2.get_obj())
                    nparr_ft_2 = np.reshape(nparr_ft_2,(dataset_size,n_rows,n_cols,n_depths)) 
                    nparr_la_2 = np.frombuffer(arr_la_2.get_obj())
                    nparr_la_2 = np.reshape(nparr_la_2,(dataset_size,n_labels))
                        
                    history = model.fit(nparr_ft_2,nparr_la_2,
                                        validation_split=validation_split, batch_size=batch_size,
                                        epochs=epochs,callbacks=callbacks_list,verbose=0)
                    
                    if DEBUG_PRINT: print("fit_train_ml end -- {}".format(time.time()-start_time))
                    sys.stdout.flush()
                    acc_list.extend(history.history['acc'])
                    val_acc_list.extend(history.history['val_acc'])
                    loss_list.extend(history.history['loss'])
                    val_loss_list.extend(history.history['loss'])
                    turn_flag = 1
        time.sleep(1)
        n_calculation = n_calculation-1

    #save the accuracy and loss graph
    save_acc_loss_graph(acc_list,val_acc_list,loss_list,val_loss_list,model_path)
    # save the model
    plot_model(model,to_file=model_path+'model.png',show_shapes=True)

def do_work_ml(target_num):
    ## setting in this function
    dataset_size = 30000
    batch_size = 256
    epochs = 50
    n_rows = 53
    n_cols = 6
    n_depths = 2
    n_labels = 4
    n_calculation = 15
    data_shape = (dataset_size,n_rows,n_cols,n_depths,n_labels)

    #initializing model!
    model = prediction_model.prediction_first_model(emr_rows=n_rows,
                                        emr_cols=n_cols,
                                        emr_depths=n_depths,
                                        num_classes=n_labels)
    
    #initializing shared array
    arr_ft_1 = Array(ctypes.c_double,dataset_size*n_rows*n_cols*n_depths)
    arr_la_1 = Array(ctypes.c_double,dataset_size*n_labels)
    arr_ft_2 = Array(ctypes.c_double,dataset_size*n_rows*n_cols*n_depths)
    arr_la_2 = Array(ctypes.c_double,dataset_size*n_labels)

    
    p_t = Process(target=train_generator_ml,
            kwargs={
            'dataset_size':dataset_size,
            'n_calculation' :n_calculation,
            'arr_ft_1' : arr_ft_1,
            'arr_la_1' : arr_la_1,
            'arr_ft_2' : arr_ft_2,
            'arr_la_2' : arr_la_2,
            'input_dir' : 'dataset_{}'.format(target_num)
            })

    p_f = Process(target=fit_train_ml,
                 kwargs={
                 'model': model, 
                 'data_shape': data_shape,
                 'arr_ft_1' : arr_ft_1,
                 'arr_la_1' : arr_la_1,
                 'arr_ft_2' : arr_ft_2,
                 'arr_la_2' : arr_la_2,
                 'n_calculation':n_calculation,
                 'epochs' : epochs,
                 'o_path' : 'train_{}'.format(target_num)
                 })

    p_t.start();p_f.start()
    p_t.join();p_f.join()
    eval_trainset(model,'/train_{}/'.format(target_num),'/dataset_{}/testset/'.format(target_num))


def _set_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('t', help="target_num")
    args = parser.parse_args()

    return args


if __name__=='__main__':
    args = _set_parser()
    do_work_ml(args.t)
