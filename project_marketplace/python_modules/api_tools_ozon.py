import pandas as pd
import numpy as np
import requests
import json
import io
import contextlib
from pathlib import Path
from PyPDF2 import PdfFileMerger
import time
from datetime import timedelta, date

from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy import text
from os import listdir

from os import getenv
from dotenv import load_dotenv
load_dotenv()
OZON_API_CID = getenv('OZON_API_CID')
OZON_API_KEY = getenv('OZON_API_KEY')

ozon_headers = {
    'Host': 'api-seller.ozon.ru',
    'Client-Id': OZON_API_CID,
    'Api-Key': OZON_API_KEY,
    'Content-Type': 'application/json'
}


def get_orders_fbo(day) -> pd.DataFrame:
    """
    Схема FBO > Список отправлений
    /v2/posting/fbo/list

    Returns dataframe with data about fbo postings on chosen day.
    """

    query = {
        'filter': {
            'since': str(day) + 'T00:00:00Z',
            'to': str(day) + 'T23:59:59Z'
        },
        'limit': 1000,
        'with': {
            'analytics_data': True,
            'financial_data': True
        }
    }

    url = 'https://api-seller.ozon.ru/v2/posting/fbo/list'

    r = requests.post(url, headers=ozon_headers, json=query)
    dict_ozon = json.loads(r.text)['result']

    if not dict_ozon:
        print('Нет заказов FBO')
        return pd.DataFrame()
    else:
        ozon_orders_fbo = pd.json_normalize(dict_ozon)

        # распаковка колонки products
        df_products = pd.DataFrame()
        for i in range(ozon_orders_fbo.shape[0]):
            row = pd.json_normalize(ozon_orders_fbo.products[i])
            row['posting_number'] = ozon_orders_fbo.posting_number[i]
            df_products = pd.concat([df_products, row])

        df_products.reset_index(inplace=True)
        df_products.drop(columns=['index'], inplace=True)

        # распаковка колонки financial_data.products
        df_financial_data_products = pd.DataFrame()
        for i in range(ozon_orders_fbo.shape[0]):
            row = pd.json_normalize(ozon_orders_fbo['financial_data.products'][i])
            row['posting_number'] = ozon_orders_fbo.posting_number[i]
            df_financial_data_products = pd.concat([df_financial_data_products, row])

        df_financial_data_products.reset_index(inplace=True)
        df_financial_data_products.drop(columns=['index', 'price', 'quantity', 'currency_code'], inplace=True)

        # соединение и присоединение df_products и financial_data.products
        df_temp = df_products.merge(df_financial_data_products,
                                    left_on=['posting_number', 'sku'],
                                    right_on=['posting_number', 'product_id'])

        ozon_orders_fbo = ozon_orders_fbo.merge(df_temp,
                                                left_on='posting_number',
                                                right_on='posting_number',
                                                how='right')

        # итоговая обработка
        ozon_orders_fbo.drop(columns=['products', 'financial_data.products'], inplace=True)
        ozon_orders_fbo['FBO/FBS'] = 'FBO'

        ozon_orders_fbo.rename(columns={
            'financial_data.posting_services.marketplace_service_item_fulfillment': 'posting_services.marketplace_service_item_fulfillment',
            'financial_data.posting_services.marketplace_service_item_pickup': 'posting_services.marketplace_service_item_pickup',
            'financial_data.posting_services.marketplace_service_item_dropoff_pvz': 'posting_services.marketplace_service_item_dropoff_pvz',
            'financial_data.posting_services.marketplace_service_item_dropoff_sc': 'posting_services.marketplace_service_item_dropoff_sc',
            'financial_data.posting_services.marketplace_service_item_dropoff_ff': 'posting_services.marketplace_service_item_dropoff_ff',
            'financial_data.posting_services.marketplace_service_item_direct_flow_trans': 'posting_services.marketplace_service_item_direct_flow_trans',
            'financial_data.posting_services.marketplace_service_item_return_flow_trans': 'posting_services.marketplace_service_item_return_flow_trans',
            'financial_data.posting_services.marketplace_service_item_deliv_to_customer': 'posting_services.marketplace_service_item_deliv_to_customer',
            'financial_data.posting_services.marketplace_service_item_return_not_deliv_to_customer': 'posting_services.marketplace_service_item_return_not_deliv_to_customer',
            'financial_data.posting_services.marketplace_service_item_return_part_goods_customer': 'posting_services.marketplace_service_item_return_part_goods_customer',
            'financial_data.posting_services.marketplace_service_item_return_after_deliv_to_customer': 'posting_services.marketplace_service_item_return_after_deliv_to_customer'},
            inplace=True)

        return ozon_orders_fbo


def orders_fbo_to_db(days_ago, db_creds, table_name):
    # получение данных по заказам
    orders_fbo = pd.DataFrame()
    for i in range(days_ago + 1):
        day = date.today() - timedelta(days=i)
        orders_fbo_temp = get_orders_fbo(day)
        orders_fbo = pd.concat([orders_fbo, orders_fbo_temp])

    # connect to db
    engine = create_engine(db_creds, echo=True)

    try:
        # удаление из базы записей старше начала периода обновления
        with engine.connect() as conn:
            stmt = text(f'''
                            DELETE FROM {table_name}
                            WHERE in_process_at::date >= '{str(date.today() - timedelta(days=days_ago))}'
                        ''')
            conn.execute(stmt)

        # добавление полученных данных в базу
        orders_fbo.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False
        )

    finally:
        engine.dispose()


def get_orders_fbs(day) -> pd.DataFrame:
    """
    Схема FBS > Список отправлений v3 /v3/posting/fbs/list
    """

    # загрузка данных из апи
    iteration = 0
    flag = True
    ozon_orders_fbs = pd.DataFrame()

    while flag:
        query = {
            'filter': {
                'since': str(day) + 'T00:00:00Z',
                'to': str(day) + 'T23:59:59Z'
            },
            'limit': 1000,
            'offset': iteration * 1000,
            'with': {
                'analytics_data': True,
                'financial_data': True
            }
        }

        url = 'https://api-seller.ozon.ru/v3/posting/fbs/list'

        r = requests.post(url, headers=ozon_headers, json=query)
        response_dict = json.loads(r.text)

        ozon_orders_fbs_temp = pd.json_normalize(response_dict['result']['postings'])
        ozon_orders_fbs = pd.concat([ozon_orders_fbs, ozon_orders_fbs_temp])

        iteration += 1

        if not response_dict['result']['has_next']:
            flag = False

    ozon_orders_fbs.reset_index(inplace=True)
    ozon_orders_fbs.drop(columns=['index'], inplace=True)

    # распаковка колонки products
    df_products = pd.DataFrame()
    for i in range(ozon_orders_fbs.shape[0]):
        row = pd.json_normalize(ozon_orders_fbs.products[i])
        row['posting_number'] = ozon_orders_fbs.posting_number[i]
        df_products = pd.concat([df_products, row])

    df_products.reset_index(inplace=True)
    df_products.drop(columns=['index'], inplace=True)

    for column in ['index', 'currency_code']:
        if column in df_products.columns:
            df_products.drop(columns=[column], inplace=True)

    # распаковка колонки financial_data.products
    df_fin_data_products = pd.DataFrame()
    for i in range(ozon_orders_fbs.shape[0]):
        row = pd.json_normalize(ozon_orders_fbs['financial_data.products'][i])
        row['posting_number'] = ozon_orders_fbs.posting_number[i]
        df_fin_data_products = pd.concat([df_fin_data_products, row])

    df_fin_data_products.reset_index(inplace=True)

    for column in ['index', 'price', 'quantity']:
        if column in df_fin_data_products.columns:
            df_fin_data_products.drop(columns=[column], inplace=True)

    # соединение и присоединение df_products и financial_data.products
    df_temp = df_products.merge(df_fin_data_products,
                                left_on=['posting_number', 'sku'],
                                right_on=['posting_number', 'product_id'])

    ozon_orders_fbs = ozon_orders_fbs.merge(df_temp,
                                            left_on='posting_number',
                                            right_on='posting_number',
                                            how='right')

    # итоговая обработка
    ozon_orders_fbs.drop(columns=['products', 'financial_data.products', 'product_id'], inplace=True)

    ozon_orders_fbs.rename(columns={
        'financial_data.posting_services.marketplace_service_item_fulfillment': 'posting_services.marketplace_service_item_fulfillment',
        'financial_data.posting_services.marketplace_service_item_pickup': 'posting_services.marketplace_service_item_pickup',
        'financial_data.posting_services.marketplace_service_item_dropoff_pvz': 'posting_services.marketplace_service_item_dropoff_pvz',
        'financial_data.posting_services.marketplace_service_item_dropoff_sc': 'posting_services.marketplace_service_item_dropoff_sc',
        'financial_data.posting_services.marketplace_service_item_dropoff_ff': 'posting_services.marketplace_service_item_dropoff_ff',
        'financial_data.posting_services.marketplace_service_item_direct_flow_trans': 'posting_services.marketplace_service_item_direct_flow_trans',
        'financial_data.posting_services.marketplace_service_item_return_flow_trans': 'posting_services.marketplace_service_item_return_flow_trans',
        'financial_data.posting_services.marketplace_service_item_deliv_to_customer': 'posting_services.marketplace_service_item_deliv_to_customer',
        'financial_data.posting_services.marketplace_service_item_return_not_deliv_to_customer': 'posting_services.marketplace_service_item_return_not_deliv_to_customer',
        'financial_data.posting_services.marketplace_service_item_return_part_goods_customer': 'posting_services.marketplace_service_item_return_part_goods_customer',
        'financial_data.posting_services.marketplace_service_item_return_after_deliv_to_customer': 'posting_services.marketplace_service_item_return_after_deliv_to_customer'},
        inplace=True)

    return ozon_orders_fbs


def orders_fbs_to_db(days_ago, db_creds, table_name):  # in progress

    # получение данных по заказам
    orders_fbs = pd.DataFrame()
    for i in range(days_ago + 1):
        day = date.today() - timedelta(days=i)
        orders_fbs_temp = get_orders_fbs(day)
        orders_fbs = pd.concat([orders_fbs, orders_fbs_temp])

    # connect to db
    engine = create_engine(db_creds, echo=True)

    try:
        # удаление из базы записей старше начала периода обновления
        with engine.connect() as conn:
            stmt = text(f'''
                            DELETE FROM {table_name}
                            WHERE in_process_at::date >= '{str(date.today() - timedelta(days=days_ago))}'
                        ''')
            conn.execute(stmt)

        # добавление полученных данных в базу
        orders_fbs.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False
        )

    finally:
        engine.dispose()


def get_prices_page(page, page_size) -> pd.DataFrame:
    url = 'https://api-seller.ozon.ru/v2/product/info/prices'

    query = {
        "page": page,
        'page_size': page_size
    }

    r = requests.post(url, headers=ozon_headers, json=query)
    dict_ozon = json.loads(r.text)['result']['items']
    df = pd.json_normalize(dict_ozon)

    return df


def get_prices() -> pd.DataFrame:
    """
    Gathering data from https://api-seller.ozon.ru/v2/product/info/prices
    """

    df_1 = get_prices_page(1, 1000)
    df_2 = get_prices_page(2, 1000)
    df_final = pd.concat([df_1, df_2])

    df_final.reset_index(inplace=True)
    df_final.drop(columns=['index'], inplace=True)

    return df_final


def get_report_csv(url: str, query: dict) -> pd.DataFrame:
    """
    Генерирует отчет по из раздела https://docs.ozon.ru/api/seller/#tag/ReportAPI

    :param url: ссылка на отчет в апи озона
    :param query: тело запроса

    :return: датафрейм с данными отчета
    """

    # Получение кода отчета
    r_1 = requests.post(url, headers=ozon_headers, json=query)
    code = json.loads(r_1.text)['result']['code']

    # Получение адреса файла с отчетом, он может быть готов не сразу
    file_url = ''
    while file_url == '':
        time.sleep(5)
        r_2 = requests.post('https://api-seller.ozon.ru/v1/report/info',
                            headers=ozon_headers,
                            json={'code': code})
        file_url = json.loads(r_2.text)['result']['file']

    # Получение файла с отчетом и его конвертация в датафрейм
    headers_cdn = {
        'Host': 'cdn1.ozone.ru',
        'Client-Id': OZON_API_CID,
        'Api-Key': OZON_API_KEY
    }

    file_csv = requests.get(file_url, headers=headers_cdn).content
    df = pd.read_csv(io.BytesIO(file_csv), sep=';')

    return df


def get_products_info() -> pd.DataFrame:
    """
    Отчеты - Отчёт по товарам /v1/report/products/create
    """

    url = 'https://api-seller.ozon.ru/v1/report/products/create'

    query = {
        'language': 'EN'
    }

    prod_info = get_report_csv(url, query)

    prod_info.rename(columns={'Артикул': 'vendor_code',
                              'Product ID': 'vendor_code',
                              'Ozon Product ID': 'Ozon_Product_ID',
                              'FBO OZON SKU ID': 'FBO_OZON_SKU_ID',
                              'FBS OZON SKU ID': 'FBS_OZON_SKU_ID',
                              'Наименование товара': 'name',
                              'Бренд': 'brand',
                              'Vendor': 'brand',
                              'Barcode': 'barcode',
                              'Статус товара': 'item_status',
                              'Видимость FBO': 'fbo_visibility',
                              'Причины скрытия FBO (при наличии)': 'fbo_reason_to_hide',
                              'Видимость FBS': 'fbs_visibility',
                              'Причины скрытия FBS (при наличии)': 'fbs_reason_to_hide',
                              'Дата создания': 'creation_date',
                              'Created at': 'creation_date',
                              'Коммерческая категория': 'category',
                              'Нужно промаркировать (кроме Твери), шт': 'need_marking',
                              'Объем товара, л': 'liters',
                              'Объемный вес, кг': 'kilograms',
                              'Доступно на складе Ozon, шт': 'ozon_stock',
                              'Зарезервировано, шт': 'ozon_reserved',
                              'Доступно на моих складах, шт': 'our_stock',
                              'Зарезервировано на моих складах, шт': 'our_reserved',
                              'Текущая цена с учетом скидки, руб.': 'price_with_discount',
                              'Цена до скидки (перечеркнутая цена), руб.': 'price_without_discount',
                              'Цена Premium, руб.': 'price_premium',
                              'Рыночная цена, руб.': 'price_market',
                              'Актуальная ссылка на рыночную цену': 'price_market_url',
                              'Вывезти и нанести КИЗ (кроме Твери), шт': 'need_kiz',
                              'Размер НДС, %': 'VAT'}, inplace=True)

    prod_info['barcode'] = prod_info.barcode.astype('str')
    prod_info['barcode'] = prod_info.barcode.apply(lambda x: x.replace('.0', ''))

    return prod_info


def products_info_db_update(table_name, db_creds):
    """
    Обновляет отчет по товарам на сегодня в базе данных.
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


def products_info_to_db_daily(table_name, db_creds):
    """
    Ежедневно сохраняет отчет по товарам.
    """
    prod_info = get_products_info()
    prod_info['date'] = date.today()

    engine = create_engine(db_creds, echo=True)

    try:
        prod_info.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False
        )

    finally:
        engine.dispose()


def get_stocks_fbo() -> pd.DataFrame:
    """
    Отчеты - Отчет по остаткам и товарам /v1/report/stock/create
    """

    query = {
        'language': 'EN'
    }

    url = 'https://api-seller.ozon.ru/v1/report/stock/create'

    ozon_stocks = get_report_csv(url, query)

    cols_with_0_sum = ozon_stocks.loc(axis=1)['Всего доступно на складах Ozon, шт':] \
        .columns[ozon_stocks.loc(axis=1)['Всего доступно на складах Ozon, шт':].apply(lambda col: col.sum() == 0)]

    ozon_stocks.drop(cols_with_0_sum, axis=1, inplace=True)

    columns_to_drop = ['Видимость сайте', 'Всего доступно на складах Ozon, шт', 'Всего зарезервировано, шт']
    for column in columns_to_drop:
        if column in ozon_stocks.columns:
            ozon_stocks.drop([column],
                             axis=1,
                             inplace=True)

    ozon_stocks = ozon_stocks.melt(
        id_vars=ozon_stocks.columns[0:6],
        var_name='warehouse_name',
        value_name='quantity')

    ozon_stocks['stock_or_reserve'] = ozon_stocks['warehouse_name'].apply(lambda x: x.split()[0])

    ozon_stocks['warehouse_name'] = ozon_stocks['warehouse_name'].apply(lambda x: x.replace('Доступно на складе ', '')
                                                                        .replace('Зарезервировано на складе ', '')
                                                                        .replace(', шт', '')
                                                                        .upper()
                                                                        .replace('САНКТ_ПЕТЕРБУРГ', 'САНКТ-ПЕТЕРБУРГ')
                                                                        .replace('РОСТОВ_НА_ДОНУ_РФЦ',
                                                                                 'РОСТОВ-НА-ДОНУ_РФЦ'))

    if 'Артикул' in ozon_stocks.columns:
        ozon_stocks.rename(columns={'Артикул': 'Product ID'}, inplace=True)

    return ozon_stocks.loc[ozon_stocks['quantity'] > 0]


def stocks_fbo_db_update(table_name, db_creds):
    engine = create_engine(db_creds, echo=True)

    try:
        with engine.connect() as conn:
            stmt = text(f'DELETE FROM {table_name}')
            conn.execute(stmt)

        ozon_stocks = get_stocks_fbo()

        ozon_stocks.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False
        )

    finally:
        engine.dispose()


def stocks_fbo_to_db_daily(table_name, db_creds):
    ozon_stocks = get_stocks_fbo()
    ozon_stocks['date'] = date.today()

    engine = create_engine(db_creds, echo=True)

    try:
        ozon_stocks.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False
        )

    finally:
        engine.dispose()


def get_transactions(day):
    """
    Финансовые отчеты - Список транзакций (версия 3) /v3/finance/transaction/list
    Возвращает два датафрейма: сам отчет и развернутую колонку services
    """

    # получение датафрейма с отчетом за выбранный день
    query = {
        'filter': {
            'date': {
                'from': str(day) + 'T00:00:00Z',
                'to': str(day) + 'T00:00:00Z'
            },
            'transaction_type': 'all'
        },
        'page': 1,
        'page_size': 10000
    }

    url = 'https://api-seller.ozon.ru/v3/finance/transaction/list'

    r = requests.post(url, headers=ozon_headers, json=query)
    r_dict = json.loads(r.text)
    ozon_transactions = pd.json_normalize(r_dict['result']['operations'])

    # распаковка колонки services в отдельный датафрейм
    ozon_transactions_services = pd.DataFrame()
    for i in range(ozon_transactions.shape[0]):
        row = pd.json_normalize(ozon_transactions['services'][i])
        row['type'] = ozon_transactions['type'][i]
        row['operation_id'] = ozon_transactions['operation_id'][i]
        row['posting_number'] = ozon_transactions['posting.posting_number'][i]
        row['operation_date'] = ozon_transactions['operation_date'][i]
        row['order_date'] = ozon_transactions['posting.order_date'][i]
        ozon_transactions_services = pd.concat([ozon_transactions_services, row])

    return ozon_transactions, ozon_transactions_services


def transactions_to_postgre(days_ago, postgre_engine_creds, transactions_table_name, transactions_services_table_name):
    # получение данных по заказам
    transactions = pd.DataFrame()
    transactions_services = pd.DataFrame()

    for i in range(days_ago + 1):
        day = date.today() - timedelta(days=i)
        transactions_temp, transactions_services_temp = get_transactions(day)
        transactions = pd.concat([transactions, transactions_temp])
        transactions_services = pd.concat([transactions_services, transactions_services_temp])

    # преобразование данных для базы
    transactions['items'] = list(map(lambda x: json.dumps(x), transactions['items']))
    transactions['services'] = list(map(lambda x: json.dumps(x), transactions['services']))

    # подключение к базе
    postgre_engine = create_engine(postgre_engine_creds, echo=True)

    try:
        # удаление из базы записей старше начала периода обновления
        with postgre_engine.connect() as conn:
            stmt = text(f'''
                            DELETE FROM {transactions_table_name}
                            WHERE operation_date::date >= '{str(date.today() - timedelta(days=days_ago))}'
                        ''')
            conn.execute(stmt)

            stmt = text(f'''
                            DELETE FROM {transactions_services_table_name}
                            WHERE operation_date::date >= '{str(date.today() - timedelta(days=days_ago))}'
                        ''')
            conn.execute(stmt)

        # добавление полученных данных в базу
        transactions.to_sql(
            transactions_table_name,
            postgre_engine,
            if_exists='append',
            index=False
        )

        transactions_services.to_sql(
            transactions_services_table_name,
            postgre_engine,
            if_exists='append',
            index=False
        )

    except Exception as err:
        print(err)

    finally:
        postgre_engine.dispose()


def get_analytics_data(date_from: date,
                       date_to: date,
                       dimensions: list,
                       metrics: list) -> pd.DataFrame:
    """
    /v1/analytics/data
    Аналитические отчеты - Данные аналитики
    Получает данные аналитики по заданным параметрам в виде датафрейма.

    :param date_from: дата начала периода
    :param date_to: дата окончания периода
    :param dimensions: список измерений
    :param metrics: список метрик

    Подробнее по допустимым значениям смотреть в документации https://docs.ozon.ru/api/seller/#operation/AnalyticsAPI_AnalyticsGetData
    """

    rows_in_response = 1000
    offset = 0
    analytical_data_final = pd.DataFrame()

    while rows_in_response == 1000:
        query = {
            'date_from': str(date_from),
            'date_to': str(date_to),
            'dimension': dimensions,
            'metrics': metrics,
            'limit': 1000,
            'offset': offset
        }

        url = 'https://api-seller.ozon.ru/v1/analytics/data'

        r = requests.post(url, headers=ozon_headers, json=query)

        try:
            while '"code":8' in r.text:
                print('Request rate limit reached. 60 sec sleep.')
                time.sleep(60)
                r = requests.post(url, headers=ozon_headers, json=query)

            r_dict = json.loads(r.text)
            data_dict = r_dict['result']['data']

        except Exception as err:
            print(err)
            print(r.text)

        # получение данных по измерениям
        dimensions_data = {}
        for i in range(len(dimensions)):
            dimensions_data[f'{dimensions[i]}_id'] = [item['dimensions'][i]['id'] for item in data_dict]
            dimensions_data[f'{dimensions[i]}_name'] = [item['dimensions'][i]['name'] for item in data_dict]

        df_dimensions = pd.DataFrame(dimensions_data)

        # получение данных по метрикам
        metrics_data = [np.array(item['metrics']) for item in data_dict]
        df_metrics = pd.DataFrame(metrics_data, columns=metrics)

        # склейка в итоговый датафрейм и добавление к финальному
        analytical_data = pd.concat([df_dimensions, df_metrics], axis=1)
        analytical_data_final = pd.concat([analytical_data_final, analytical_data])

        rows_in_response = len(data_dict)
        offset += 1000
        print(offset)

        time.sleep(15)

    if 'day_name' in analytical_data_final.columns:
        analytical_data_final.drop(columns='day_name', inplace=True)

    return analytical_data_final


def analytics_data_to_postgre(days_ago: int,
                              dimensions: list,
                              metrics: list,
                              postgre_engine_creds: str,
                              table_name: str):
    """
    Получает данные аналитики по заданным параметрам в виде датафрейма и загружает в postgre с заменой по дате.

    :param days_ago: на сколько дней назад брать период
    :param dimensions: список измерений
    :param metrics: список метрик
    :param postgre_engine_creds: строка с конфигурацией для логина через sqlalchemy
    :param table_name: название таблицы в базе в которую грузятся данные
    """

    date_from = date.today() - timedelta(days=days_ago)

    analytics_data = get_analytics_data(date_from, date.today(), dimensions, metrics)

    # подключение к базе
    postgre_engine = create_engine(postgre_engine_creds, echo=True)

    try:
        # удаление из базы записей старше начала периода обновления
        with postgre_engine.connect() as conn:
            stmt = text(f'''
                            DELETE FROM {table_name}
                            WHERE day_id::date >= '{str(date_from)}'
                        ''')
            conn.execute(stmt)

        # добавление в базу обновленных данных
        analytics_data.to_sql(
            table_name,
            postgre_engine,
            if_exists='append',
            index=False
        )

    except Exception as err:
        print(err)

    finally:
        postgre_engine.dispose()


def get_stickers_and_table(csv_from_lk, db_engine, path_to_temp_folder, path_to_result_folder):
    """
    Создает пдф файл со стикерами и таблицу для сборки. Учитывается нужная для склада сортировка.

    :param csv_from_lk: str, path object or file-like object. В таблице должны быть колонки: "Номер отправления", "Артикул", "Количество"
    :param db_engine: движок базы данных
    :param path_to_temp_folder: путь к папке для временных файлов (пути передавать со слешом в конце)
    :param path_to_result_folder: путь к папке, куда сохранится итоговая таблица и пдф со стикерами
    """
    # загрузка таблицы с заказами из лк озона
    input_csv = pd.read_csv(csv_from_lk, delimiter=';')

    # загрузка 1с инфы из базы данных
    sql_query = '''
                SELECT
                    vendor_code,
                    barcode,
                    brand,
                    item_name,
                    characteristic,
                    cell
                FROM public.products_info_1c
                '''

    prod_info_1c = pd.read_sql(sql_query, db_engine)

    # удаление ненужных данных из озоновской таблицы и форматирование
    orders_fbs_postings = input_csv[['Номер отправления', 'Артикул', 'Количество']]
    orders_fbs_postings['Артикул'] = orders_fbs_postings['Артикул'].astype('str')
    orders_fbs_postings.rename(columns={'Номер отправления': 'posting_number',
                                        'Артикул': 'vendor_code',
                                        'Количество': 'quantity'}, inplace=True)

    # мердж с данными из 1с
    orders_fbs_postings = orders_fbs_postings.merge(prod_info_1c, how='left', on='vendor_code')

    # сортировка для склада
    posting_sku_count = orders_fbs_postings.groupby(['posting_number'], as_index=False).agg(
        {'vendor_code': 'count'}).sort_values('vendor_code', ascending=True)
    posting_sku_count_1 = posting_sku_count[posting_sku_count['vendor_code'] == 1]
    posting_sku_count_more_than_1 = posting_sku_count[posting_sku_count['vendor_code'] > 1]

    orders_fbs_postings_1 = orders_fbs_postings[
        orders_fbs_postings.posting_number.isin(list(posting_sku_count_1.posting_number))].sort_values('item_name')
    orders_fbs_postings_more_than_1 = orders_fbs_postings[
        orders_fbs_postings.posting_number.isin(list(posting_sku_count_more_than_1.posting_number))].sort_values(
        ['posting_number',
         'item_name'])
    orders_fbs_postings = pd.concat([orders_fbs_postings_1, orders_fbs_postings_more_than_1])

    # выгрузка эксельки
    orders_fbs_postings = orders_fbs_postings[
        ['posting_number', 'item_name', 'characteristic', 'quantity', 'vendor_code', 'cell', 'barcode']]
    orders_fbs_postings.to_excel(f'{path_to_result_folder}ozon_stickers_table.xlsx', index=False)

    # запрос в апи озона не генерацию стикеров по списку номеров отправлений
    posting_numbers_list = list(orders_fbs_postings.posting_number.drop_duplicates())

    url = 'https://api-seller.ozon.ru/v2/posting/fbs/package-label'

    # генерация пдфок по каждому отправлению и их сохранение в папку
    for i in range(len(posting_numbers_list)):
        query = {"posting_number": [posting_numbers_list[i]]}

        r = requests.post(url, headers=ozon_headers, json=query)

        if 'INVALID_ARGUMENT' in r.text:
            print(f'{i} {posting_numbers_list[i]} INVALID_ARGUMENT')
            continue

        file_bytes = r.content

        with open(f'{path_to_temp_folder}sticker-{i:04d}.pdf', 'wb') as file:
            file.write(file_bytes)

        print(f'{i} {posting_numbers_list[i]}')

    # объединение первых страниц всех пдфок в одну и её сохранение
    list_of_pdfs = listdir(path_to_temp_folder)

    with contextlib.ExitStack() as stack:
        merger = PdfFileMerger()
        files = [stack.enter_context(open(f'{path_to_temp_folder}{pdf}', 'rb')) for pdf in list_of_pdfs]

        for f in files:
            merger.append(fileobj=f, pages=(0, 1))
        with open(f'{path_to_result_folder}ozon_stickers.pdf', 'wb') as f:
            merger.write(f)

    # очистка папки с пдфками
    [f.unlink() for f in Path(path_to_temp_folder).glob('*') if f.is_file()]
