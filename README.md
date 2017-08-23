## Hypernatremia_EMR

### 1. Objective

​	EMR 데이터를 바탕으로, 환자의 나트륨 변화를 예측하는 연구

### 2. directory explanation

 - **data** : EMR　데이터를　저장．전처리결과물　등　data인　모든　것들은　이　폴더에　저장
 - **converter** : mapping table을　생성하는　모듈．
- **mapapplier** :　converter에서　생성된　mapping table을　raw data에　적용하는　모듈
- **generator** :　환자　번호별로　EMR matrix를　생성하는　모듈

### 3. data

연구　： 저혈증나트륨　환자　중　혈청　나트륨검사를　시행한　적이　있는　모든　환자（L3041소디움/ L8041소디움:응급）

기간　： 2010.1 ~ 2016.12(7년)

___

**Writer :** Kang Sang Jae, Researcher & engineer in Mediwhale 

**Period :** '17.07.30 ~

