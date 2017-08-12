### sampling_data.py

* -i  (input_path) : 샘플링할　데이터　
* -o (output_path) : 샘플링된　데이터가　저장될　위치
* -l  (line_counts) :  샘플링할　라인 갯수　
* -skip (skip count) : 데이터　프레임　전체　세는　것을　생략　（row & col 갯수　셈）

```shell
python sampling_data.py -i ./disease.dat -o ./sample_disease.dat -l 1000000 －skip true
```

