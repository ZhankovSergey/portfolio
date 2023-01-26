from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from os import getenv
from dotenv import load_dotenv
load_dotenv()
MODULES_DIR = getenv('MODULES_DIR')
WB_API_KEY = getenv('WB_API_KEY')
PG_BF_USER_ENGINE = getenv('PG_BF_USER_ENGINE')
GOOGLE_JSON_ON_SERVER = getenv('GOOGLE_JSON_ON_SERVER')
GOOGLE_SHEET_ID_PROD_INFO_1C = getenv('GOOGLE_SHEET_ID_PROD_INFO_1C')
GOOGLE_SHEET_ID_WB_WRONG_ART = getenv('GOOGLE_SHEET_ID_WB_WRONG_ART')

import sys
sys.path.append(MODULES_DIR)
import bi_tools, db_tools
import api_tools_ozon, api_tools_wb, api_tools_yandex

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2021, 12, 19),
    'retries': 0
}

dag = DAG('bf_mp_products_info_to_db',
          default_args=default_args,
          schedule_interval='30 2 * * *')

products_info_1c_to_db = PythonOperator(task_id='products_info_1c_to_db',
                                        python_callable=bi_tools.products_info_1c_postgre_update,
                                        op_kwargs={
                                            'google_creds_path': GOOGLE_JSON_ON_SERVER,
                                            'spreadsheet_id': GOOGLE_SHEET_ID_PROD_INFO_1C,
                                            'table_name': 'products_info_1c',
                                            'engine_creds': PG_BF_USER_ENGINE
                                        },
                                        dag=dag)

ozon_products_info_to_db = PythonOperator(task_id='ozon_products_info_to_db',
                                          python_callable=api_tools_ozon.products_info_db_update,
                                          op_kwargs={
                                              'table_name': 'ozon_products_info',
                                              'db_creds': PG_BF_USER_ENGINE
                                          },
                                          dag=dag)

ozon_stocks_fbo_to_db = PythonOperator(task_id='ozon_stocks_fbo_to_db',
                                       python_callable=api_tools_ozon.stocks_fbo_db_update,
                                       op_kwargs={
                                           'table_name': 'ozon_stocks_fbo',
                                           'db_creds': PG_BF_USER_ENGINE
                                       },
                                       dag=dag)

t4 = PythonOperator(task_id='wb_products_info_to_db',
                    python_callable=api_tools_wb.products_info_db_update,
                    op_kwargs={
                        'db_creds': PG_BF_USER_ENGINE,
                        'table_name': 'wb_products_info'
                    },
                    dag=dag)

t5 = PythonOperator(task_id='wb_stat_stocks_to_db',
                    python_callable=api_tools_wb.stat_stocks_db_update,
                    op_kwargs={
                        'db_creds': PG_BF_USER_ENGINE,
                        'table_name': 'wb_stat_stocks'
                    },
                    dag=dag)

wb_stocks_to_db = PythonOperator(task_id='wb_stocks_to_db',
                                 python_callable=api_tools_wb.stocks_db_update,
                                 op_kwargs={
                                     'db_creds': getenv('PG_BF_USER_ENGINE'),
                                     'table_name': 'wb_stocks'
                                 },
                                 dag=dag)

t6 = PythonOperator(task_id='yandex_products_info_to_db',
                    python_callable=api_tools_yandex.products_info_db_update,
                    op_kwargs={
                        'db_creds': getenv('PG_BF_USER_ENGINE'),
                        'table_name': 'yandex_products_info'
                    },
                    dag=dag)

t7 = PythonOperator(task_id='yandex_skus_info_to_db',
                    python_callable=api_tools_yandex.skus_info_db_update,
                    op_kwargs={
                        'db_creds': getenv('PG_BF_USER_ENGINE'),
                        'table_name': 'yandex_skus_info'
                    },
                    dag=dag)

t8 = PythonOperator(task_id='wb_products_info_update_wrong_articles',
                    python_callable=db_tools.update_wrong_articles,
                    op_kwargs={
                        'engine_creds': PG_BF_USER_ENGINE,
                        'db_table': 'wb_products_info',
                        'db_table_article_col_name': 'article',
                        'google_sheet_id': GOOGLE_SHEET_ID_WB_WRONG_ART,
                        'google_sheet_worksheet_num': 0,
                        'google_token_path': GOOGLE_JSON_ON_SERVER
                    },
                    dag=dag)

t9 = PythonOperator(task_id='wb_products_info_update_wrong_articles_size',
                    python_callable=db_tools.update_wrong_articles_with_2nd_condition,
                    op_kwargs={
                        'engine_creds': PG_BF_USER_ENGINE,
                        'db_table': 'wb_products_info',
                        'db_table_article_col_name': 'article',
                        'second_col_name': 'size',
                        'google_sheet_id': GOOGLE_SHEET_ID_WB_WRONG_ART,
                        'google_sheet_worksheet_num': 1,
                        'google_token_path': GOOGLE_JSON_ON_SERVER
                    },
                    dag=dag)

t10 = PythonOperator(task_id='wb_stat_stocks_update_wrong_articles',
                     python_callable=db_tools.update_wrong_articles,
                     op_kwargs={
                        'engine_creds': PG_BF_USER_ENGINE,
                        'db_table': 'wb_stat_stocks',
                        'db_table_article_col_name': 'supplierArticle',
                        'google_sheet_id': GOOGLE_SHEET_ID_WB_WRONG_ART,
                        'google_sheet_worksheet_num': 0,
                        'google_token_path': GOOGLE_JSON_ON_SERVER
                     },
                     dag=dag)

t11 = PythonOperator(task_id='wb_prices_info_to_db',
                     python_callable=api_tools_wb.prices_info_db_update,
                     op_kwargs={
                        'db_creds': PG_BF_USER_ENGINE,
                        'table_name': 'wb_prices_info'
                     },
                     dag=dag)

t4 >> [t8, t9]
t5 >> t10
