from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, date, timedelta

from os import getenv
from dotenv import load_dotenv
load_dotenv()
MODULES_DIR = getenv('MODULES_DIR')
PG_BF_USER_ENGINE = getenv('PG_BF_USER_ENGINE')

import sys
sys.path.append(MODULES_DIR)
import api_tools_wb

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 12),
    'retries': 1
}

dag = DAG('mp_realization_to_db',
          default_args=default_args,
          schedule_interval='20 5 * * 2')


wb_realization = PythonOperator(task_id='wb_realization_to_db',
                                python_callable=api_tools_wb.realization_to_postgre,
                                op_kwargs={
                                    'date_from': date.today()-timedelta(days=8),
                                    'date_to': date.today()-timedelta(days=2),
                                    'db_creds': PG_BF_USER_ENGINE,
                                    'table_name': 'wb_realization'
                                },
                                dag=dag)
