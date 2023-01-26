from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from os import getenv
from dotenv import load_dotenv
load_dotenv()
MODULES_DIR = getenv('MODULES_DIR')
PG_BF_USER_ENGINE = getenv('PG_BF_USER_ENGINE')
CH_BF_USER_ENGINE = getenv('CH_BF_USER_ENGINE')
PG_BF_USER_PASS = getenv('PG_BF_USER_PASS')

from sys import path
path.append(MODULES_DIR)
import db_tools
import db_tools_ch

detach_cur_month_part = '''
                        ALTER TABLE bi_orders DETACH PARTITION tuple(toYYYYMM(now()))
                        '''

insert_cur_month_part = f'''
                         INSERT INTO bi_orders SELECT * 
                         FROM postgresql('82.146.43.173:5432', 'bestfit', 'bi_orders', 'bestfit_user', {PG_BF_USER_PASS}) 
                         WHERE toYYYYMM(creation_date) = toYYYYMM(now())
                         '''

detach_last_month_part = '''
                         ALTER TABLE bi_orders DETACH PARTITION tuple(toYYYYMM(timestamp_sub(month, 1, now())))
                         '''

insert_last_month_part = f'''
                          INSERT INTO bi_orders SELECT * 
                          FROM postgresql('82.146.43.173:5432', 'bestfit', 'bi_orders', 'bestfit_user', {PG_BF_USER_PASS}) 
                          WHERE toYYYYMM(creation_date) = toYYYYMM(timestamp_sub(month, 1, now()))
                          '''

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 28),
    'retries': 1
}

dag = DAG('views_and_rel_ch_table_update',
          default_args=default_args,
          schedule_interval='30 0/3 * * *')

t1 = PythonOperator(task_id='pg_update_view_bi_orders',
                      python_callable=db_tools.update_mat_view,
                      op_kwargs={
                          'view_name': 'bi_orders',
                          'db_creds': PG_BF_USER_ENGINE
                      },
                      dag=dag)

t2 = PythonOperator(task_id='ch_upd_bi_orders_detach_cur_month',
                      python_callable=db_tools_ch.execute_query,
                      op_kwargs={
                          'query': detach_cur_month_part,
                          'db_creds': CH_BF_USER_ENGINE
                      },
                      dag=dag)

t3 = PythonOperator(task_id='ch_upd_bi_orders_insert_cur_month',
                      python_callable=db_tools_ch.execute_query,
                      op_kwargs={
                          'query': insert_cur_month_part,
                          'db_creds': CH_BF_USER_ENGINE
                      },
                      dag=dag)

t4 = PythonOperator(task_id='ch_upd_bi_orders_detach_last_month',
                      python_callable=db_tools_ch.execute_query,
                      op_kwargs={
                          'query': detach_last_month_part,
                          'db_creds': CH_BF_USER_ENGINE
                      },
                      dag=dag)

t5 = PythonOperator(task_id='ch_upd_bi_orders_insert_last_month',
                      python_callable=db_tools_ch.execute_query,
                      op_kwargs={
                          'query': insert_last_month_part,
                          'db_creds': CH_BF_USER_ENGINE
                      },
                      dag=dag)

t6 = PythonOperator(task_id='pg_update_view_monitor_prices_with_statuses',
                      python_callable=db_tools.update_mat_view,
                      op_kwargs={
                          'view_name': 'monitor_prices_with_statuses',
                          'db_creds': PG_BF_USER_ENGINE
                      },
                      dag=dag)

t1 >> t2 >> t3 >> t4 >> t5
