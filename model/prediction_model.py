# -*- coding: utf-8 -*-
import sys, os, re
import time
import random

os_path = os.path.abspath('./') ; find_path = re.compile('emr_hypernatremia')
BASE_PATH = os_path[:find_path.search(os_path).span()[1]]
sys.path.append(BASE_PATH)

import keras
from keras.models import Sequential
from keras.layers import Input, Dense, Dropout, Activation,Flatten
from keras.layers.pooling import MaxPooling2D
from keras.layers.convolutional import Conv2D
from keras.layers.normalization import BatchNormalization
from keras.layers import concatenate
from keras.models import Model

from keras.optimizers import SGD
from keras.optimizers import Adam
from keras import initializers

def prediction_first_model(emr_rows,emr_cols,emr_depths,num_classes):
    input_shape = Input(shape=(emr_rows,emr_cols,emr_depths))
    # 3months(long term predicition)
    model_1 = MaxPooling2D((1,3),strides=(1,2),padding='same')(input_shape)
    model_1 = Conv2D(8,(53,3), padding='same',kernel_initializer=initializers.he_uniform())(model_1)
    model_1 = BatchNormalization()(model_1)
    model_1 = Activation('relu')(model_1)
    # 2months(middle term predicition)
    model_2 = MaxPooling2D((1,2),strides=(1,2),padding='same')(input_shape)
    model_2 = Conv2D(8,(53,3), padding='same',kernel_initializer=initializers.he_uniform())(model_2)
    model_2 = BatchNormalization()(model_2)
    model_2 = Activation('relu')(model_2)
    # 1months(short term predicition)
    model_3 = Conv2D(8,(53,3), padding='same',kernel_initializer=initializers.he_uniform())(input_shape)
    model_3 = BatchNormalization()(model_3)
    model_3 = Activation('relu')(model_3)
    model_3 = MaxPooling2D((1,2),strides=(1,2),padding='same')(model_3)
    model_3 = Conv2D(8,(53,3), padding='same',kernel_initializer=initializers.he_uniform())(model_3)
    model_3 = BatchNormalization()(model_3)
    model_3 = Activation('relu')(model_3)

    merged = concatenate([model_1,model_2,model_3],axis=1)
    merged = Flatten()(merged)

    merged = Dropout(0.5)(merged)
    merged = Dense(100,activation='relu')(merged)
    merged = Dropout(0.5)(merged)
    merged = Dense(100,activation='relu')(merged)
    merged = BatchNormalization()(merged)

    out = Dense(num_classes,activation='softmax')(merged)

    model = Model(inputs=input_shape, outputs=out)
    adam = Adam(lr=0.001, beta_1=0.9, beta_2=0.999, epsilon=1e-08, decay=0.0)
    model.compile(optimizer=adam,loss='categorical_crossentropy',metrics=['accuracy'])

    return model