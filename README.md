# de_airflow_w10
### DAG 작성 실습
<ol>

<li>songeun_w10_CountryInfo.py</li> <br>
api를 통해 아래의 정보를 읽어와 Redshift에 테이블을 생성하고 적재하는 DAG <br>
Full Refresh <br>
UTC로 매주 토요일 오전 6시 30분에 실행되도록 스케줄링 <br>
<br>
country: 국가명<br>
population: 인구수<br>
area: 국토 면적<br>

<br><br>

<li>MySQL_to_Redshift_v3.py</li> <br>
MySQL에서 데이터를 읽어와 s3에 적재한 뒤 Redshift로 Bulk Update하는 DAG <br>
기존에 맥스님께서 제공하신 MySQL_to_Redshift_v2.py 파일에서 나의 schema로 변경하고, schema 밑에 nps 테이블 생성하는 task를 추가 <br>
Incremental <br>
<br>

</ol>
