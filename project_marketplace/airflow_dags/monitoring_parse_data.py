from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from os import getenv
from dotenv import load_dotenv
load_dotenv()
MODULES_DIR = getenv('MODULES_DIR')
CHROME_DRIVER_PATH = getenv('CHROME_DRIVER_PATH')
PG_BF_USER_ENGINE = getenv('PG_BF_USER_ENGINE')

import sys
sys.path.append(MODULES_DIR)
import monitor_tools_wb

wb_price_limit = 14000

wb_category_url_list = [
            'https://www.wildberries.ru/catalog/sport/sportivnoe-pitanie?sort=popular&page=1',
            'https://www.wildberries.ru/catalog/pitanie/napitki?sort=popular&page=1&xsubject=3382%3B3441',
            'https://www.wildberries.ru/catalog/pitanie/sneki?sort=popular&page=1&xsubject=3014',
            'https://www.wildberries.ru/catalog/pitanie/sladosti/shokolad-i-shokoladnye-batonchiki?sort=popular&page=1&xsubject=2878',
            'https://www.wildberries.ru/catalog/pitanie/sladosti/shokolad-i-shokoladnye-batonchiki?sort=popular&page=1&xsubject=3568',
            'https://www.wildberries.ru/catalog/produkty/dobavki-pishchevye?sort=popular&page=1',
            'https://www.wildberries.ru/catalog/produkty/sladosti/orehi?sort=popular&page=1&xsubject=3407',
            'https://www.wildberries.ru/catalog/produkty/sladosti/pasty?sort=popular&page=1',
            'https://www.wildberries.ru/catalog/dom-i-dacha/zdorove/vitaminy-i-bady?sort=popular&page=1',
            'https://www.wildberries.ru/catalog/zdorove/lechebnoe-pitanie-?sort=popular&page=1'
]

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 6),
    'retries': 1
}

dag = DAG('monitoring_parse_data',
          default_args=default_args,
          schedule_interval='0 6 * * 7')

t1 = PythonOperator(task_id='wb_save_our_stock_data',
                    python_callable=monitor_tools_wb.save_our_stock_data,
                    op_kwargs={
                        'db_creds': PG_BF_USER_ENGINE,
                        'chrome_driver_path': CHROME_DRIVER_PATH,
                        'pkl_backup_path': '/home/analyst1/www/price_monitor/wb/wb_our_stock_data.pkl'
                    },
                    dag=dag)

t2 = PythonOperator(task_id='wb_save_product_list_from_cat_pages',
                    python_callable=monitor_tools_wb.save_product_list_from_cat_pages,
                    op_kwargs={
                        'chrome_driver_path': CHROME_DRIVER_PATH,
                        'pkl_backup_path': '/home/analyst1/www/price_monitor/wb/wb_prod_list.pkl',
                        'category_url_list': wb_category_url_list,
                        'price_limit': wb_price_limit
                    },
                    dag=dag)

t3 = PythonOperator(task_id='wb_save_prod_list_json_data',
                    python_callable=monitor_tools_wb.save_prod_list_with_json_data,
                    op_kwargs={
                        'prod_list_pkl_path': '/home/analyst1/www/price_monitor/wb/wb_prod_list.pkl',
                        'wb_prod_list_data_pkl_save_path': '/home/analyst1/www/price_monitor/wb/wb_prod_list_data.pkl'
                    },
                    dag=dag)

t1 >> t2 >> t3
