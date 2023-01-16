from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from os import getenv
from dotenv import load_dotenv
load_dotenv()
PROXY_GER = getenv('PROXY_GER')
CHROME_DRIVER_PATH = getenv('CHROME_DRIVER_PATH')
MODULES_DIR = getenv('MODULES_DIR')

import sys
sys.path.append(MODULES_DIR)
import monitor_tools_other_sites

iherb_url_list = ['https://kg.iherb.com/c/california-gold-nutrition',
                  'https://kg.iherb.com/c/azelique',
                  'https://kg.iherb.com/c/lake-avenue-nutrition',
                  'https://kg.iherb.com/c/oslomega',
                  'https://kg.iherb.com/c/radiant-seoul',
                  'https://kg.iherb.com/c/sierra-bees',
                  'https://kg.iherb.com/c/solumeve',
                  'https://kg.iherb.com/c/super-nutrition',
                  'https://kg.iherb.com/c/ultamins',
                  'https://kg.iherb.com/c/zoi-research-products']

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 18),
    'retries': 1
}

dag = DAG('monitoring_parse_data_from_other_sites',
          default_args=default_args,
          schedule_interval='0 20 * * *')

t1 = PythonOperator(task_id='iherb_parse_data',
                    python_callable=monitor_tools_other_sites.iherb_parse_catalog,
                    op_kwargs={
                        'url_list': iherb_url_list,
                        'chrome_driver_path': CHROME_DRIVER_PATH,
                        'excel_save_path': '/home/analyst1/www/price_monitor/iherb_products_data.xlsx',
                        'chrome_proxy': PROXY_GER
                    },
                    dag=dag)
