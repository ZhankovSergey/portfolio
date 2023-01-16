from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys

from os import getenv
from dotenv import load_dotenv
load_dotenv()
MODULES_DIR = getenv('MODULES_DIR')
CHROME_DRIVER_PATH = getenv('CHROME_DRIVER_PATH')
PG_BF_USER_ENGINE = getenv('PG_BF_USER_ENGINE')

sys.path.append(MODULES_DIR)
import monitor_tools_wb

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 6),
    'retries': 1
}

dag = DAG('monitoring_update_prices',
          default_args=default_args,
          schedule_interval='0 9,21 * * *')

t1 = PythonOperator(task_id='wb_price_update',
                    python_callable=monitor_tools_wb.prices_update,
                    op_kwargs={
                        'engine_creds': PG_BF_USER_ENGINE,
                        'chrome_driver_path': CHROME_DRIVER_PATH,
                    },
                    dag=dag)

t2 = PythonOperator(task_id='our_wb_price_update',
                    python_callable=monitor_tools_wb.our_prices_update,
                    op_kwargs={
                        'engine_creds': PG_BF_USER_ENGINE,
                        'chrome_driver_path': CHROME_DRIVER_PATH,
                    },
                    dag=dag)
