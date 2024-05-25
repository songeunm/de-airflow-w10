# de_airflow_w10
데브코스 데이터 엔지니어링 10주차 실습 과제

세계 나라 정보 API를 사용하여 Full Refresh로 매번 국가 정보를 읽어오는 DAG를 작성한다.

<li>api 결과에서 아래 세개의 정보를 추출하여 Redshift에 각자 스키마 밑에 테이블을 생성해 데이터 적재<br>
  country -> ["name"]["official"]<br>
  population -> ["population"]<br>
  area -> ["area"]<br>

<li>DAG는 UTC로 매주 토요일 오전 6시 30분에 실행되도록 스케줄링
