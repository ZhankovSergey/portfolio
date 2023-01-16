from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from os import getenv
from dotenv import load_dotenv
load_dotenv()
MODULES_DIR = getenv('MODULES_DIR')
PG_BF_USER_ENGINE = getenv('PG_BF_USER_ENGINE')

import sys
sys.path.append(MODULES_DIR)
import api_tools_ozon, api_tools_wb, api_tools_yandex, api_tools_sber

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2022, 8, 10),
    'retries': 0
}

dag = DAG('mp_orders_to_db',
          default_args=default_args,
          schedule_interval='5 0/6 * * *')

ozon_orders_fbo = PythonOperator(task_id='ozon_orders_fbo',
                                 python_callable=api_tools_ozon.orders_fbo_to_db,
                                 op_kwargs={
                                    'days_ago': 20,
                                    'db_creds': PG_BF_USER_ENGINE,
                                    'table_name': 'ozon_orders_fbo'
                                 },
                                 dag=dag)

ozon_orders_fbs = PythonOperator(task_id='ozon_orders_fbs',
                                 python_callable=api_tools_ozon.orders_fbs_to_db,
                                 op_kwargs={
                                    'days_ago': 20,
                                    'db_creds': PG_BF_USER_ENGINE,
                                    'table_name': 'ozon_orders_fbs'
                                 },
                                 dag=dag)

wb_orders = PythonOperator(task_id='wb_orders',
                           python_callable=api_tools_wb.orders_to_db,
                           op_kwargs={
                               'days_ago': 20,
                               'db_creds': PG_BF_USER_ENGINE,
                               'db_name': 'wb_orders'
                           },
                           dag=dag)

wb_orders_fbs = PythonOperator(task_id='wb_orders_fbs',
                               python_callable=api_tools_wb.orders_fbs_to_db,
                               op_kwargs={
                                   'days_ago': 20,
                                   'engine_creds': PG_BF_USER_ENGINE,
                                   'table_name': 'wb_orders_fbs'
                               },
                               dag=dag)

yandex_orders_fbs = PythonOperator(task_id='yandex_orders_fbs',
                                   python_callable=api_tools_yandex.orders_fbs_to_db,
                                   op_kwargs={
                                       'days_ago': 20,
                                       'db_creds': getenv('PG_BF_USER_ENGINE'),
                                       'table_name': 'yandex_orders_fbs'
                                   },
                                   dag=dag)

sber_orders = PythonOperator(task_id='sber_orders',
                             python_callable=api_tools_sber.orders_to_db,
                             op_kwargs={
                                 'days_ago': 20,
                                 'db_creds': PG_BF_USER_ENGINE,
                                 'table_name': 'sber_orders'
                             },
                             dag=dag)
