#!/bin/sh

#　데이터　셋을　만드는　함수

## OFFSET COUNTS 변화에　따른　데이터　셋
echo "start offset COUNTS!-1" > ./process.txt
python generate_input_mp.py ../data/input/dataset_2 na_label 4 7 4000 6 1 3 25 50
echo "start offset COUNTS!-2" >> ./process.txt
python generate_input_mp.py ../data/input/dataset_3 na_label 4 7 4000 6 1 3 50 75
echo "start offset COUNTS!-3" >> ./process.txt
python generate_input_mp.py ../data/input/dataset_4 na_label 4 7 4000 6 1 3 75 100
echo "start offset COUNTS! end" >> ./process.txt

## gap period 변화에　따른　데이터　셋
echo "start gap period!-1" >> ./process.txt 
python generate_input_mp.py ../data/input/dataset_5 na_label 4 7 4000 6 2 3 100 1000
echo "start gap period!-2" >> ./process.txt 
python generate_input_mp.py ../data/input/dataset_6 na_label 4 7 4000 6 3 3 100 1000
echo "start gap period!-3" >> ./process.txt 
python generate_input_mp.py ../data/input/dataset_7 na_label 4 7 4000 6 6 3 100 1000
echo "gap period end!" >> ./process.txt 

## time length 변화에　따른　데이터셋
echo "start time length!-1" >> ./process.txt
python generate_input_mp.py ../data/input/dataset_8 na_label 4 7 4000 6 1 3 20 50
echo "start time length!-2" >> ./process.txt
python generate_input_mp.py ../data/input/dataset_9 na_label 4 7 4000 6 1 3 20 50
echo "start time length!-3" >> ./process.txt
python generate_input_mp.py ../data/input/dataset_10 na_label 4 7 4000 6 1 3 20 50
echo "time length end!" >> ./process.txt

