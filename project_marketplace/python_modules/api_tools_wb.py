from datetime import timedelta, datetime, date, timezone
import time
import pandas as pd
import requests
import json
from sqlalchemy import create_engine
from sqlalchemy import text

from os import getenv
from dotenv import load_dotenv
load_dotenv()
WB_API_KEY = getenv('WB_API_KEY')
WB_API_KEY_X64 = getenv('WB_API_KEY_X64')


def get_order_statuses(orders_list: list) -> pd.DataFrame:
    """
    Получает датафрейм со статусами сборочных заданий по методу /api/v3/orders/status
    """

    headers = {
        'accept': 'application/json',
        'Authorization': WB_API_KEY,
        'Content-Type': 'application/json'
    }

    url = 'https://suppliers-api.wildberries.ru/api/v3/orders/status'

    orders_status = pd.DataFrame()
    current_order_list_len = 1000
    i = 0

    while current_order_list_len == 1000:
        json = {
            'orders': orders_list[i * 1000:i * 1000 + 1000]
        }

        r = requests.post(url, headers=headers, json=json)
        orders_status = orders_status.append(pd.DataFrame(r.json()['orders']))

        current_order_list_len = len(orders_list[i * 1000:i * 1000 + 1000])
        i += 1

    return orders_status


def get_products_info() -> pd.DataFrame:

    prod_info = pd.DataFrame()
    i = 0

    headers = {
            'accept': 'application/json',
            'Authorization': WB_API_KEY
        }

    url = 'https://suppliers-api.wildberries.ru/api/v2/stocks'

    while True:
        params = {
            'skip': 1000 * i,
            'take': '1000'
        }

        r = requests.get(url, params=params, headers=headers)

        data = json.loads(r.text)
        if data['stocks'] is None:
            break

        prod_info_temp = pd.json_normalize(data['stocks'])
        prod_info = pd.concat([prod_info, prod_info_temp])

        i += 1

    prod_info.drop(columns='barcodes', inplace=True)

    return prod_info


def products_info_db_update(db_creds, table_name, api_key):
    """
    Updates db table (wb_products_info)
    """

    prod_info = get_products_info()

    engine = create_engine(db_creds, echo=True)

    try:
        with engine.connect() as conn:
            stmt = text(f'DELETE FROM {table_name}')
            conn.execute(stmt)

        prod_info.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False
        )

    finally:
        engine.dispose()


def products_info_to_db_daily(db_creds, table_name, api_key):
    """
    Updates db table (wb_products_info_daily)
    """

    products_info = get_products_info()
    products_info['date'] = date.today()

    engine = create_engine(db_creds, echo=True)

    try:
        products_info.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False
        )

    finally:
        engine.dispose()


def get_warehouses_list() -> pd.DataFrame:
    """
    Отдает датафрейм со списком складов из отчета /api/v2/warehouses
    """

    headers = {
        'accept': 'application/json',
        'Authorization': WB_API_KEY
    }

    url = 'https://suppliers-api.wildberries.ru/api/v2/warehouses'

    r = requests.get(url, headers=headers)
    data = json.loads(r.text)
    wb_report = pd.json_normalize(data)

    return wb_report


def get_stat_stocks():
    day = date.today() - timedelta(days=30)

    url = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/stocks'

    params = {
        'dateFrom': str(day),
        'key': getenv('WB_API_KEY_X64')
    }

    r = requests.get(url, params=params)
    data = r.json()
    wb_stocks_old = pd.json_normalize(data)

    return wb_stocks_old


def stat_stocks_db_update(db_creds, table_name):

    prod_info = get_stat_stocks()

    engine = create_engine(db_creds, echo=True)

    try:
        with engine.connect() as conn:
            stmt = text(f'DELETE FROM {table_name}')
            conn.execute(stmt)

        prod_info.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False
        )

    finally:
        engine.dispose()


def stat_stocks_to_db_daily(db_creds, table_name):

    stat_stocks = get_stat_stocks()
    stat_stocks['date'] = date.today()

    engine = create_engine(db_creds, echo=True)

    try:
        stat_stocks.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False
        )

    finally:
        engine.dispose()


def get_stocks() -> pd.DataFrame:
    """
    Возвращает датафрейм с данными из отчета /api/v3/stocks/{warehouse}
    """

    stat_stocks = get_stat_stocks()
    stat_stocks_barcode_list = list(stat_stocks.barcode.drop_duplicates())

    wh_list = get_warehouses_list()

    stocks = pd.DataFrame()
    for warehouse in wh_list.values:
        for barcode in stat_stocks_barcode_list:
            headers = {
                'accept': 'application/json',
                'Authorization': getenv('WB_API_KEY'),
                'Content-Type': 'application/json'
            }

            params = {
                "skus": [barcode]
            }

            url = f'https://suppliers-api.wildberries.ru/api/v3/stocks/{warehouse[1]}'

            r = requests.post(url=url, json=params, headers=headers)
            data = json.loads(r.text)['stocks']

            stocks_temp = pd.json_normalize(data)
            stocks_temp['wh_name'] = warehouse[0]
            stocks_temp['wh_id'] = warehouse[1]

            stocks = stocks.append(stocks_temp)

    stocks['wh_id'] = stocks['wh_id'].astype('str')
    stocks['amount'] = stocks['amount'].astype('int')

    return stocks


def stocks_db_update(db_creds, table_name):

    stocks = get_stocks()

    engine = create_engine(db_creds, echo=True)

    try:
        with engine.connect() as conn:
            stmt = text(f'DELETE FROM {table_name}')
            conn.execute(stmt)

        stocks.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False
        )

    finally:
        engine.dispose()


def stocks_to_db_daily(db_creds, table_name):

    stocks = get_stocks()
    stocks['date'] = date.today()

    engine = create_engine(db_creds, echo=True)

    try:
        stocks.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False
        )

    finally:
        engine.dispose()


def get_prices_info() -> pd.DataFrame:
    """
    Получение информации по всем товарам, их ценам, скидкам и промокодам.
    """

    headers = {
        'accept': 'application/json',
        'Authorization': WB_API_KEY
    }

    params = {
        'quantity': 0
    }
    url = 'https://suppliers-api.wildberries.ru/public/api/v1/info'

    r = requests.get(url, params=params, headers=headers)
    data = json.loads(r.text)
    wb_price_info = pd.json_normalize(data)

    wb_price_info = wb_price_info.astype({'nmId': 'str'})
    wb_price_info['price_with_disc'] = round(wb_price_info.price * (wb_price_info.discount / 100), 1)

    return wb_price_info


def prices_info_db_update(db_creds, table_name):

    prices = get_prices_info()

    engine = create_engine(db_creds, echo=True)

    try:
        with engine.connect() as conn:
            stmt = text(f'DELETE FROM {table_name}')
            conn.execute(stmt)

        prices.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False
        )

    finally:
        engine.dispose()


def get_orders(day: date, db_creds: str) -> pd.DataFrame:
    """
    Методы статистики > Заказы /api/v1/supplier/orders

    :param day: RFC339
    :param db_creds: строка вида 'postgresql+psycopg2://user:password@host:port/db_name'
    """

    url = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/orders'

    params = {
        'dateFrom': str(day),
        'flag': 1,
        'key': getenv('WB_API_KEY_X64')
    }

    r = requests.get(url, params=params)
    while r.text == '' or r.text == '{"errors":["(api-new) too many requests"]}':
        time.sleep(30)
        r = requests.get(url, params=params)

    if r.text == '[]':
        return pd.DataFrame()

    data = r.json()
    orders = pd.json_normalize(data)

    # add new columns
    orders['quantity'] = 1
    orders['price_with_disc'] = orders.totalPrice * ((100 - orders.discountPercent) / 100)
    orders['order_status'] = orders['isCancel'].apply(lambda x: 'cancelled' if x == True else 'not cancelled')

    # get list of 'rids' from wb_orders_fbs db table
    db_engine = create_engine(db_creds, echo=True)
    query = text(f'''
                    SELECT rid FROM wb_orders_fbs
                    WHERE "createdAt"::date >= '{str(day - timedelta(days=5))}' 
                      AND "createdAt"::date <= '{str(day + timedelta(days=5))}'
                ''')

    wb_orders_fbs_rid_list = list(pd.read_sql_query(sql=query, con=db_engine).rid)
    db_engine.dispose()

    # compare and write fulfillment type in 'fulfillment' column
    orders['fulfillment'] = orders['srid'].apply(lambda x: 'fbs' if (x in wb_orders_fbs_rid_list) or (x == '') else 'fbo')

    return orders


def orders_to_db(days_ago, db_creds, db_name):
    # get orders data from api
    orders_fbo = pd.DataFrame()
    for i in range(days_ago + 1):
        time.sleep(1)
        day = date.today() - timedelta(days=i)
        orders_fbo_temp = get_orders(day, db_creds)
        orders_fbo = pd.concat([orders_fbo, orders_fbo_temp])

    # connect to db
    db_engine = create_engine(db_creds, echo=True)

    try:
        # delete from db data that older than update period
        with db_engine.connect() as conn:
            stmt = text(f'''
                            DELETE FROM {db_name}
                            WHERE "date"::date >= '{str(date.today() - timedelta(days=days_ago))}'
                        ''')
            conn.execute(stmt)

        # add new fresh data to db
        orders_fbo.to_sql(
            db_name,
            db_engine,
            if_exists='append',
            index=False)

    finally:
        db_engine.dispose()


def get_orders_fbs(day: date) -> pd.DataFrame:
    """
    /api/v3/orders
    Получение сборочных заданий за определенный день.
    """

    orders_fbs = pd.DataFrame()
    next_value = 0
    rows = 1000

    headers = {
        'accept': 'application/json',
        'Authorization': WB_API_KEY
    }

    url = 'https://suppliers-api.wildberries.ru/api/v3/orders'

    while rows == 1000:
        params = {
            'dateFrom': int(
                datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=timezone.utc).timestamp()),
            'dateTo': int(
                datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc).timestamp()),
            'limit': 1000,
            'next': next_value
        }

        r = requests.get(url, params=params, headers=headers)
        r_data = r.json()

        if not r_data['orders']:
            return pd.DataFrame()

        orders_fbs_temp = pd.json_normalize(r_data['orders'])
        orders_fbs = pd.concat([orders_fbs, orders_fbs_temp])

        next_value = r_data['next']
        rows = orders_fbs_temp.shape[0]

    # финальные преобразования в более удобный формат
    orders_fbs['price'] = orders_fbs['price'] / 100
    orders_fbs['convertedPrice'] = orders_fbs['convertedPrice'] / 100
    orders_fbs['barcode'] = orders_fbs['skus'].astype('str').str.strip("[]").str.replace("'", "")

    return orders_fbs


def orders_fbs_to_db(days_ago, engine_creds, table_name):
    """
    Add wb fbs orders to db.
    """

    # получение данных по заказам
    orders = pd.DataFrame()
    for i in range(days_ago + 1):
        day = date.today() - timedelta(days=i)
        orders_temp = get_orders_fbs(day)
        orders = pd.concat([orders, orders_temp])

    engine = create_engine(engine_creds, echo=True)

    try:
        # удаление из базы записей старше начала периода обновления
        with engine.connect() as conn:
            stmt = text(f'''
                            DELETE FROM {table_name}
                            WHERE "createdAt"::date >= '{str(date.today() - timedelta(days=days_ago))}'
                        ''')
            conn.execute(stmt)

        # загрузка в базу сежих данных
        orders.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False)

    finally:
        engine.dispose()


def get_sales(day: date, flag: int, db_creds: str) -> pd.DataFrame:
    """
    Методы статистики > Продажи
    /api/v1/supplier/sales

    Аргумент flag может принимать два значения:
        0 - возвращаются данные у которых значение поля lastChangeDate больше переданного в вызов значения параметра day
        1 - будет выгружена информация обо всех заказах или продажах с датой равной переданному параметру day

    :param day: RFC339
    :param flag: int
    :param db_creds: string like 'postgresql+psycopg2://user:password@host:port/db_name'
    """

    url = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/sales'

    params = {
        'dateFrom': str(day),
        'flag': flag,
        'key': getenv('WB_API_KEY_X64')
    }

    r = requests.get(url, params=params)

    while 'too many requests' in r.text:
        time.sleep(30)
        r = requests.get(url, params=params)

    r_data = r.json()
    sales = pd.json_normalize(r_data)

    # get list of 'rids' from wb_orders_fbs db table
    db_engine = create_engine(db_creds, echo=True)
    query = text(f'''
                    SELECT rid FROM wb_orders_fbs
                    WHERE "createdAt"::date >= '{str(day - timedelta(days=15))}' 
                      AND "createdAt"::date <= '{str(day + timedelta(days=2))}'
                ''')

    wb_orders_fbs_rid_list = list(pd.read_sql_query(sql=query, con=db_engine).rid)
    db_engine.dispose()

    # compare and write fulfillment type in 'fulfillment' column
    sales['fulfillment'] = sales['srid'].apply(lambda x: 'fbs' if (x in wb_orders_fbs_rid_list) or (x == '') else 'fbo')

    return sales


def sales_to_db(days_ago, db_creds, db_name):
    """
    Add wb sales to db.
    """

    # получение данных по продажам
    sales = pd.DataFrame()
    for i in range(days_ago + 1):
        day = date.today() - timedelta(days=i)
        sales_temp = get_sales(day, 1, db_creds)
        sales = pd.concat([sales, sales_temp])

    # connect to db
    db_engine = create_engine(db_creds, echo=True)

    try:
        # delete from db data that older than update period
        with db_engine.connect() as conn:
            stmt = text(f'''
                            DELETE FROM {db_name}
                            WHERE "date"::date >= '{str(date.today() - timedelta(days=days_ago))}'
                        ''')
            conn.execute(stmt)

        # добавление полученных данных в базу
        sales.to_sql(
            db_name,
            db_engine,
            if_exists='append',
            index=False
        )

    finally:
        db_engine.dispose()


def get_realization(date_from: date, date_to: date) -> pd.DataFrame:
    """
    Получить отчет о продажах по реализации /api/v1/supplier/reportDetailByPeriod

    :param date_from: начало периода
    :param date_to: конец периода

    :return: датафрейм с данными отчета за выбранный период
    """

    rrdid = 0
    last_num_of_rows = 100000
    sales_realization = pd.DataFrame()

    headers = {
        'accept': 'application/json'
    }

    url = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailByPeriod'

    while last_num_of_rows == 100000:
        params = {
            'key': WB_API_KEY_X64,
            'dateFrom': str(date_from),
            'dateTo': str(date_to),
            'rrdid': int(rrdid)
        }

        r = requests.get(url, params=params, headers=headers)

        while 'too many requests' in r.text:
            time.sleep(60)
            r = requests.get(url, params=params, headers=headers)

        r_dict = json.loads(r.text)
        sales_realization_temp = pd.json_normalize(r_dict)
        sales_realization = pd.concat([sales_realization, sales_realization_temp])

        rrdid = sales_realization_temp['rrd_id'].max()
        last_num_of_rows = sales_realization_temp.shape[0]
        print(sales_realization.shape)

    return sales_realization


def realization_to_postgre(date_from: date, date_to: date, table_name: str, db_creds: str):
    """
    Обновляет данные отчета о продажах по реализации в базе.

    :param date_from: начало периода
    :param date_to: конец периода
    :param table_name: название обновляемой таблицы в бд
    :param db_creds: строка для авторизации в бд вроде 'postgresql+psycopg2://user:password@host:port/db_name'
    """

    # получение данных по продажам
    realization = get_realization(date_from, date_to)

    # подключение к базе
    db_engine = create_engine(db_creds, echo=True)

    try:
        # удаление старых данных за добавляемый период
        with db_engine.connect() as conn:
            stmt = text(f'''
                            DELETE FROM {table_name}
                            WHERE date_from::date = '{str(date_from)}'
                        ''')
            conn.execute(stmt)

        # добавление новых данных
        realization.to_sql(
            table_name,
            db_engine,
            if_exists='append',
            index=False
        )

    finally:
        db_engine.dispose()
