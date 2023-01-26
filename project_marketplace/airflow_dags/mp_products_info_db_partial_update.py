from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys

from os import getenv
from dotenv import load_dotenv
load_dotenv()
MODULES_DIR = getenv('MODULES_DIR')
WB_API_KEY = getenv('WB_API_KEY')
PG_BF_USER_ENGINE = getenv('PG_BF_USER_ENGINE')

sys.path.append(MODULES_DIR)
import db_tools
import api_tools_ozon, api_tools_wb

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 7),
    'retries': 1
}

dag = DAG('bf_mp_products_info_partial_update',
          default_args=default_args,
          schedule_interval='20 9,13,17 * * *')

t1 = PythonOperator(task_id='ozon_products_info_stocks_update',
                    python_callable=db_tools.update_stock_if_bigger,
                    op_args=[],
                    op_kwargs={
                        'func': api_tools_ozon.get_products_info,
                        'db_creds': PG_BF_USER_ENGINE,
                        'db_table': 'ozon_products_info_daily',
                        'db_date_col_name': 'date',
                        'stocks_col_name': 'our_stock',
                        'article_col_name': 'vendor_code'
                    },
                    dag=dag)

t2 = PythonOperator(task_id='wb_products_info_stocks_update',
                    python_callable=db_tools.update_stock_if_bigger,
                    op_kwargs={
                        'func': api_tools_wb.get_products_info,
                        'db_creds': PG_BF_USER_ENGINE,
                        'db_table': 'wb_products_info_daily',
                        'db_date_col_name': 'date',
                        'stocks_col_name': 'stock',
                        'article_col_name': 'id'
                    },
                    dag=dag)

t3 = PythonOperator(task_id='wb_stocks_update',
                    python_callable=db_tools.update_stock_if_bigger,
                    op_kwargs={
                        'func': api_tools_wb.get_stocks,
                        'db_creds': PG_BF_USER_ENGINE,
                        'db_table': 'wb_stocks_daily',
                        'db_date_col_name': 'date',
                        'stocks_col_name': 'amount',
                        'article_col_name': 'sku'
                    },
                    dag=dag)

# t4 = PythonOperator(task_id='yandex_skus_info_stocks_update',
#                     python_callable=db_tools.update_stock_if_bigger,
#                     op_args=[],
#                     op_kwargs={
#                         'func': api_tools_yandex.get_skus_info,
#                         'db_creds': PG_BF_USER_ENGINE,
#                         'db_table': 'yandex_skus_info_daily',
#                         'db_date_col_name': 'date',
#                         'stocks_col_name': 'stock_plus_reserved',
#                         'article_col_name': 'shopSku'
#                     },
#                     dag=dag)

t1 >> t2 >> t3
