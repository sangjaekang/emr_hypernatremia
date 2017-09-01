#!/bin/sh

#　데이터　셋을　만드는　함수
set_1=false
set_2=false
set_3=false
set_4=true

## OFFSET COUNTS 변화에　따른　데이터　셋

if $set_1 ; then

now=$(date + "%T")
echo "Current time : $now"

echo "start offset COUNTS!-1" > ./process.txt
rm -rf ../data/input/dataset_2
python generate_input_mp.py ../data/input/dataset_2 na_label 4 7 4000 6 1 3 5 25

now=$(date + "%T")
echo "Current time : $now"

echo "start offset COUNTS!-2" >> ./process.txt
rm -rf ../data/input/dataset_3
python generate_input_mp.py ../data/input/dataset_3 na_label 4 7 4000 6 1 3 25 50

now=$(date + "%T")
echo "Current time : $now"

echo "start offset COUNTS!-3" >> ./process.txt
rm -rf ../data/input/dataset_4
python generate_input_mp.py ../data/input/dataset_4 na_label 4 7 4000 6 1 3 50 75

now=$(date + "%T")
echo "Current time : $now"

echo "start offset COUNTS! end" >> ./process.txt
fi

## gap period 변화에　따른　데이터　셋

if $set_2 ; then

now=$(date + "%T")
echo "Current time : $now"

echo "start gap period!-1" >> ./process.txt 
rm -rf ../data/input/dataset_5
python generate_input_mp.py ../data/input/dataset_5 na_label 4 7 4000 6 2 3 100 1000

now=$(date + "%T")
echo "Current time : $now"

echo "start gap period!-2" >> ./process.txt 
rm -rf ../data/input/dataset_6
python generate_input_mp.py ../data/input/dataset_6 na_label 4 7 4000 6 3 3 100 1000

now=$(date + "%T")
echo "Current time : $now"

echo "start gap period!-3" >> ./process.txt 
rm -rf ../data/input/dataset_7
python generate_input_mp.py ../data/input/dataset_7 na_label 4 7 4000 6 6 3 100 1000

now=$(date + "%T")
echo "Current time : $now"

echo "gap period end!" >> ./process.txt 
fi

## time length 변화에　따른　데이터셋

if $set_3 ; then

now=$(date + "%T")
echo "Current time : $now"

echo "start time length!-1" >> ./process.txt
rm -rf ../data/input/dataset_8
python generate_input_mp.py ../data/input/dataset_8 na_label 4 7 4000 6 1 3 100 1000

now=$(date + "%T")
echo "Current time : $now"

echo "start time length!-2" >> ./process.txt
rm -rf ../data/input/dataset_9
python generate_input_mp.py ../data/input/dataset_9 na_label 4 7 4000 9 1 3 150 1000

now=$(date + "%T")
echo "Current time : $now"

echo "start time length!-3" >> ./process.txt
rm -rf ../data/input/dataset_10
python generate_input_mp.py ../data/input/dataset_10 na_label 4 7 4000 12 1 3 200 1000

now=$(date + "%T")
echo "Current time : $now"

echo "time length end!" >> ./process.txt
fi

if $set_4 ; then

now=$(date + "%T")
echo "Current time : $now"

echo "start Ka label!" >> ./process.txt
rm -rf ../data/input/dataset_11
python generate_input_mp.py ../data/input/dataset_11 ka_label 4 7 4000 6 1 3 100 2000

echo "start Ka label!" >> ./process.txt
rm -rf ../data/input/dataset_12
python generate_input_mp.py ../data/input/dataset_12 ka_label 4 7 4000 6 1 3 50 100


echo "start Ka label!" >> ./process.txt
rm -rf ../data/input/dataset_13
python generate_input_mp.py ../data/input/dataset_13 ka_label 4 7 4000 6 1 3 20 50


now=$(date + "%T")
echo "Current time : $now"


fi
