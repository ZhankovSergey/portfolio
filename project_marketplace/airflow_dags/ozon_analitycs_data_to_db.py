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
import api_tools_ozon

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 6),
    'retries': 1
}

dag = DAG('ozon_analytical_data_to_db',
          default_args=default_args,
          schedule_interval='0 1 * * *')

dimensions = ['day', 'sku']

metrics_hit_view = [
            'hits_view_search',
            'hits_view_pdp',
            'hits_view',
            'hits_tocart_search',
            'hits_tocart_pdp',
            'hits_tocart',
            'session_view_search',
            'session_view_pdp',
            'session_view',
            'adv_view_pdp',
            'adv_view_search_category',
            'adv_view_all',
            'adv_sum_all',
            'position_category'
]

metrics_orders = [
            'revenue',
            'returns',
            'cancellations',
            'ordered_units',
            'delivered_units',
            'postings',
            'postings_premium'
]

t1 = PythonOperator(task_id='hits_views',
                    python_callable=api_tools_ozon.analytics_data_to_postgre,
                    op_kwargs={
                        'days_ago': 5,
                        'dimensions': dimensions,
                        'metrics': metrics_hit_view,
                        'postgre_engine_creds': PG_BF_USER_ENGINE,
                        'table_name': 'ozon_analytics_data_hit_view'
                    },
                    dag=dag)

t2 = PythonOperator(task_id='orders',
                    python_callable=api_tools_ozon.analytics_data_to_postgre,
                    op_kwargs={
                        'days_ago': 5,
                        'dimensions': dimensions,
                        'metrics': metrics_orders,
                        'postgre_engine_creds': PG_BF_USER_ENGINE,
                        'table_name': 'ozon_analytics_data_orders'
                    },
                    dag=dag)
