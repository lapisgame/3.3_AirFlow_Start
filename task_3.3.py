import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

import psycopg2
import requests
import datetime

# database credentials
db_name = 'test'
db_user = 'postgres'
db_pass = 'password'
db_host = 'db'
db_port = '5432'

db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)

# rates table
sql_string = '''
            create table if not exists rates
            (
	            ticker varchar(32),
                rate_date varchar(32),
	            rate varchar(32)
            );'''


# вспомогательные функции
def create_table():
    with psycopg2.connect(db_string) as conn:
        cursor = conn.cursor()
        cursor.execute(sql_string)
        conn.commit()
    conn.close()   

def rate_get():
    url = 'https://api.exchangerate.host/latest?'
    response = requests.get(url)
    data = response.json()['rates']
    rate = data['RUB'] / data['BTC']

    dt = datetime.datetime.now()
    
    return ('BTC', dt.strftime('%H:%M - %m.%d.%Y'), rate)

def data_insert(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='get_rate')

    with psycopg2.connect(db_string) as conn:
        cursor = conn.cursor()
        cursor.execute(
                f"INSERT INTO rates ({', '.join(['ticker', 'rate_date', 'rate'])}) VALUES ({', '.join(['%s'] * 3)})",
                data
            )
        conn.commit()
    conn.close()   


#Объявление DAG
default_args = {
    'owner': 'airflow',    
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

task_33_new = DAG(
    dag_id = 'task_33_new',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    dagrun_timeout=timedelta(minutes=60),
    description='task 3.2 decision',
    start_date = airflow.utils.dates.days_ago(1)
)

print_line = BashOperator(
    task_id="print_line",
    bash_command='echo Good morning my diggers!',
    dag=task_33_new
)

create_task = PythonOperator(
    task_id='create_task',
    python_callable=create_table,
    provide_context=True,
    dag = task_33_new
    )

get_rate = PythonOperator(
    task_id='get_rate',
    python_callable=rate_get,
    provide_context=True,
    dag = task_33_new
    )

insert_data = PythonOperator(
    task_id='insert_data',
    python_callable=data_insert,
    provide_context=True,
    dag = task_33_new
    )

print_line >> create_task >> get_rate >> insert_data