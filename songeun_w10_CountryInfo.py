from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

import logging
import requests


# Redshift에 connect
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def extract_transform():
    ####
    # 세계 나라 정보 API를 사용해 국가 정보(country, population, area) 읽어오기
    ####
    url = 'https://restcountries.com/v3/all'
    req = requests.get(url)
    json_data = req.json()

    records = []
    for data in json_data:
        c = data['name']['official']
        p = data['population']
        a = data['area']
        if "\'" in c:
            c = c.replace("\'", "\'\'") # single quote escape 처리
            print(c)
        records.append([c, p, a])

    return records

@task
def load(schema, table, records):
    ####
    # 읽어온 정보를 Redshift에 Full Refresh로 적재
    ####
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
CREATE TABLE {schema}.{table} (
    country varchar(256) primary key,
    population int,
    area float
);
""")
        
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES ('{r[0]}', {r[1]}, {r[2]});"
            print(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    
    logging.info("load done")


with DAG(
    dag_id = 'CountryInfo',
    start_date = datetime(2024,5,25),
    catchup = False,
    tags = ['API'],
    schedule = '30 6 * * 6'
) as dag:

    results = extract_transform()
    load("thdrms_dl", "country_info", results)
