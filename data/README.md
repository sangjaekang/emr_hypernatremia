### sampling_data.py

> 데이터에서　라인　갯수만큼　샘플링하여　저장하는　함수

**input parameter**

* -i  (input_path) : 샘플링할　데이터　
* -o (output_path) : 샘플링된　데이터가　저장될　위치
* -l  (line_counts) :  샘플링할　라인 갯수　
* -skip (skip count) : 데이터　프레임　전체　세는　것을　생략　（row & col 갯수　셈）

````shell
python sampling_data.py -i [input file] -i [output file] -l [line number] -skip [true/false]
````

예시

```shell
python sampling_data.py -i ./disease.dat -o ./sample_disease.dat -l 1000000 －skip true
```

### sorting

``` shell
sort --field-separator=[DELIMITIER] --key=[해당　칼럼　넘버] [input file] -o [output file]
```

CSV　파일을　shell 로　sorting　하는　방법．속도가　빠름．

예시

````shell
sort　--field-separator=',' --key=1 disease.dat -o sorted_disease.dat 
````

###  converting to utf-8

````shell
iconv -f ORIGINALENCODING -t utf-8 ORIGINALFILE > TARGETFILE
````

TXT　파일을　utf-8로　바꾸는　방법．　속도가　빠름

예시

````shell
iconv -f cp949 -t utf-8 disease.dat > c_disease.dat
rm disease.dat 
cv c_disease.dat > disease.dat
````

