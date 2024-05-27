from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import json

dag = DAG(
    dag_id = 'MySQL_to_Redshift_v3',
    start_date = datetime(2023,1,1), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 9 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

schema = "thdrms_dl"
table = "nps"
s3_bucket = "grepp-data-engineering"
s3_key = schema + "-" + table       # s3_key = schema + "/" + table






def create_table(**context):
    ####
    # 나의 스키마 밑에 nps 테이블이 없는 경우(처음) 생성
    ####
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = True
    conn.cursor().execute(f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        id int NOT NULL primary key,
        created_at timestamp,
        score smallint
    );""")


create_table = PythonOperator(
    task_id = 'create_table',
    python_callable = create_table,
    params = {},
    dag = dag
)






sql = "SELECT * FROM prod.nps WHERE DATE(created_at) = DATE('{{ execution_date }}')"
print(sql)
mysql_to_s3_nps = SqlToS3Operator(
    task_id = 'mysql_to_s3_nps',
    query = sql,
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    sql_conn_id = "mysql_conn_id",
    aws_conn_id = "aws_conn_id",
    verify = False,
    replace = True,
    pd_kwargs={"index": False, "header": False},    
    dag = dag
)

s3_to_redshift_nps = S3ToRedshiftOperator(
    task_id = 's3_to_redshift_nps',
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    schema = schema,
    table = table,
    copy_options=['csv'],
    redshift_conn_id = "redshift_dev_db",
    aws_conn_id = "aws_conn_id",    
    method = "UPSERT",
    upsert_keys = ["id"],
    dag = dag
)

create_table >> mysql_to_s3_nps >> s3_to_redshift_nps
