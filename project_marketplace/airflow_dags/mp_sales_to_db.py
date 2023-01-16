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
import api_tools_ozon, api_tools_wb

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2022, 8, 10),
    'retries': 1
}

dag = DAG('mp_sales_to_db',
          default_args=default_args,
          schedule_interval='45 6 * * *')

ozon_transactions = PythonOperator(task_id='ozon_transactions_to_db',
                                   python_callable=api_tools_ozon.transactions_to_postgre,
                                   op_kwargs={
                                       'days_ago': 20,
                                       'postgre_engine_creds': PG_BF_USER_ENGINE,
                                       'transactions_table_name': 'ozon_transactions',
                                       'transactions_services_table_name': 'ozon_transactions_services'
                                   },
                                   dag=dag)

wb_sales = PythonOperator(task_id='wb_sales_to_db',
                          python_callable=api_tools_wb.sales_to_db,
                          op_kwargs={
                              'days_ago': 15,
                              'db_creds': PG_BF_USER_ENGINE,
                              'db_name': 'wb_sales'
                          },
                          dag=dag)
