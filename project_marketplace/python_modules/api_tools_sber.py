import pandas as pd
import requests
import json
import time
from datetime import timedelta, date
from sqlalchemy import create_engine
from sqlalchemy import text

from os import getenv
from dotenv import load_dotenv
load_dotenv()
SBER_API_KEY = getenv('SBER_API_KEY')


def get_shipments_list(request_date: date) -> pd.DataFrame:
    """
    Получение списка отправлений за определенную дату.
    :param request_date: дата оформления отправлений
    :return: список с номерами отправлений
    """

    headers = {
        'Content-Type': 'application/json'
    }

    query = {
        "data": {
            "token": SBER_API_KEY,
            "dateFrom": str(request_date) + 'T00:00:00Z',
            "dateTo": str(request_date) + 'T23:59:59Z',
            "statuses": []
        },
        "meta": {}
    }

    url = 'https://partner.sbermegamarket.ru/api/market/v1/orderService/order/search'

    r = requests.get(url, headers=headers, json=query)
    while json.loads(r.text)['success'] == 0:
        time.sleep(1)
        r = requests.get(url, headers=headers, json=query)

    shipments_list = json.loads(r.text)['data']['shipments']

    return shipments_list


def get_orders(request_date: date) -> pd.DataFrame:
    """
    Отдает данные по заказам за определенную дату.
    :param request_date: дата оформления заказов
    :return: pandas df
    """

    # получение списка отправлений
    shipments_list = get_shipments_list(request_date)

    # получение данных по заказам по списку отправлений
    headers = {
        'Content-Type': 'application/json'
    }

    query = {
        "data": {
            "token": SBER_API_KEY,
            "shipments": shipments_list
        },
        "meta": {}
    }

    url = 'https://partner.sbermegamarket.ru/api/market/v1/orderService/order/get'

    r = requests.get(url, headers=headers, json=query)
    while json.loads(r.text)['success'] == 0:
        time.sleep(1)
        r = requests.get(url, headers=headers, json=query)

    orders = pd.DataFrame(json.loads(r.text)['data']['shipments'])

    # распаковка колонки items
    orders_items = pd.DataFrame()
    for i in range(orders.shape[0]):
        row = pd.json_normalize(orders['items'][i])
        row['shipmentId'] = orders.shipmentId[i]
        orders_items = pd.concat([orders_items, row])

    # объединение и дроп ненужных колонок
    orders.drop(columns='status', inplace=True)
    orders = orders.merge(orders_items, how='right', on='shipmentId')
    orders.drop(columns='items', inplace=True)

    return orders


def orders_to_db(days_ago, db_creds, table_name):

    # получение данных по заказам
    orders = pd.DataFrame()
    for i in range(days_ago + 1):
        day = date.today() - timedelta(days=i)
        orders_temp = get_orders(day)
        orders = pd.concat([orders, orders_temp])

    # форматирование колонок со словарями для возможности загрузки в базу данных
    if 'discounts' in orders.columns:
        orders['discounts'] = list(map(lambda x: json.dumps(x), orders['discounts']))
    if 'events' in orders.columns:
        orders['events'] = list(map(lambda x: json.dumps(x), orders['events']))
    if 'priceAdjustments' in orders.columns:
        orders['priceAdjustments'] = list(map(lambda x: json.dumps(x), orders['priceAdjustments']))

    engine = create_engine(db_creds, echo=True)

    try:
        # удаление из базы записей старше начала периода обновления
        with engine.connect() as conn:
            stmt = text(f'''
                            DELETE FROM {table_name}
                            WHERE "creationDate"::date >= '{str(date.today() - timedelta(days=days_ago))}'
                        ''')
            conn.execute(stmt)

        # загрузка новых данных в базу
        orders.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False
        )

    finally:
        engine.dispose()
