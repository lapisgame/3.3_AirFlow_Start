import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from urllib.parse import urlencode

import psycopg2
import requests
import datetime
import calendar


# database credentials
host = 'localhost'
port = '5430'
user = 'postgres'
password = 'password'
database = 'test'

headers = ['currency', 'basic_curr', 'rate_date', 'rate'] 
mart_headers = ["валюта", "день_макс", "день_мин", "курс_макс", "курс_мин", "курс_сред", "курс_посл"]


# вспомогательные функции
def create_sql(ticker):
    return f'''create table if not exists ticker_{ticker} (
        row_id serial,
        rate_date varchar(32),
        currency varchar(32),
        basic_curr varchar(32),
        rate varchar(32)
);'''


def data_mart_string(ticker):
	return f'''
	
	with tmp_maxx ("валюта", "день_макс", "курс_макс") as (
		select distinct currency, rate_date, rate 
		from ticker_{ticker}
		where rate = (select max(rate) from ticker_{ticker})
		limit 1
	),

	tmp_minn ("день_мин", "курс_мин") as (
		select distinct rate_date, rate
		from ticker_{ticker}
		where rate = (select min(rate) from ticker_{ticker})
		limit 1
	),

	tmp_aver ("средний курс") as (select avg(rate::numeric)::varchar from ticker_{ticker}),

	tmp_lday ("последний курс") as (
		select rate
		from ticker_{ticker}
		order by rate_date desc
		limit 1
	)

	select * from tmp_maxx, tmp_minn, tmp_aver, tmp_lday;
'''

def main():
    data_mart_table = '''
	create table if not exists curr_report (
            "валюта" varchar(32), 
            "день_макс" varchar(32),
            "курс_макс" varchar(32),
            "день_мин" varchar(32),
            "курс_мин" varchar(32),
            "курс_сред" varchar(32),
            "курс_посл" varchar(32)
	);
    '''
    

    symbols = ['RUB', 'USD', 'EUR', 'GBP', 'AED', 'AFN']
    base = 'BTC'

    now = datetime.datetime.now()

    _, eend = calendar.monthrange(now.year, now.month)
    mon = f'0{now.month}' if now.month < 10 else now.month


    base_url = 'https://api.exchangerate.host/timeseries?'
    params = { 
        'start_date':f'{now.year}-{mon}-01',
        'end_date':f'{now.year}-{mon}-{eend}',
        'symbols':','.join(symbols),
        'base': base   
    }

    response = requests.get(base_url + urlencode(params))
    data = response.json()['rates']

    _ = []
    for k, v in data.items():
        for k1 in v.keys():        
            _.append((k1, base, k, v[k1]))
            

    for symb in symbols:
        data = [line for line in _ if line[0] == symb]
        
        create = create_sql(symb)
        data_mart = data_mart_string(symb)
    
        
        with psycopg2.connect(host=host, port=port, user=user, password=password, database=database) as conn:
            cursor = conn.cursor()
            cursor.execute(create)
            conn.commit()

            for row in data:
                cursor.execute(f"insert into ticker_{symb} ({', '.join(headers)}) values ({', '.join(['%s'] * len(headers))})", row)
                conn.commit()     
            print(f'insert into ticker_{symb} OK')  
        conn.close()

    report_data = list()

    for symb in symbols:
        with psycopg2.connect(host=host, port=port, user=user, password=password, database=database) as conn:
            data_mart = data_mart_string(symb)
            
            cursor = conn.cursor()
            cursor.execute(data_mart)
            report_data.append(cursor.fetchone())
            conn.commit()
        conn.close()

    with psycopg2.connect(host=host, port=port, user=user, password=password, database=database) as conn:
        cursor = conn.cursor()
        cursor.execute(data_mart_table)
        conn.commit()
                
        for row in report_data:
            cursor.execute(f"INSERT INTO curr_report VALUES ({', '.join(['%s'] * len(mart_headers))})", row)

    conn.commit()

    
#Объявление DAG
default_args = {
    'owner': 'airflow',    
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

task_33 = DAG(
    dag_id = 'task_33',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    dagrun_timeout=timedelta(minutes=60),
    description='task 3.2 decision',
    start_date = airflow.utils.dates.days_ago(1)
)

Good_morning = BashOperator(
    task_id="Good_morning",
    bash_command='echo Good morning my diggers!',
    dag=task_33
)

main_task = PythonOperator(
    task_id='main_task',
    python_callable=main,
    provide_context=True,
    dag = task_33
)

Good_morning >> main_task