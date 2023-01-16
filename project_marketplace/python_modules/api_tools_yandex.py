import pandas as pd
import numpy as np
import requests
import json
from datetime import timedelta, date
from sqlalchemy import create_engine
from sqlalchemy import text

from os import getenv
from dotenv import load_dotenv
load_dotenv()
YA_OAUTH_TOKEN = getenv('YA_OAUTH_TOKEN')
YA_OAUTH_CID = getenv('YA_OAUTH_CID')
YA_CAMPAIGN_ID = getenv('YA_CAMPAIGN_ID')

headers = {'Authorization': f'OAuth oauth_token="{YA_OAUTH_TOKEN}", '
                            f'oauth_client_id="{YA_OAUTH_CID}"'}


def get_products_info() -> pd.DataFrame:

    url = f'https://api.partner.market.yandex.ru/v2/campaigns/{YA_CAMPAIGN_ID}/offer-mapping-entries.json'

    products_info = pd.DataFrame()
    next_page = ''
    try:
        while True:
            params = {
                'campaignId': YA_CAMPAIGN_ID,
                'limit': 200,
                'page_token': next_page
            }

            r = requests.get(url, params=params, headers=headers)
            dict_yandex = json.loads(r.text)['result']['offerMappingEntries']
            df_temp = pd.json_normalize(dict_yandex)
            products_info = pd.concat([products_info, df_temp])

            next_page = json.loads(r.text)['result']['paging']['nextPageToken']

    finally:
        products_info['offer.barcodes'] = products_info['offer.barcodes'] \
            .apply(lambda x: json.dumps(x))
        products_info['offer.pictures'] = products_info['offer.pictures'] \
            .apply(lambda x: json.dumps(x))
        products_info['offer.manufacturerCountries'] = products_info['offer.manufacturerCountries'] \
            .apply(lambda x: json.dumps(x, ensure_ascii=False))
        products_info['offer.urls'] = products_info['offer.urls'] \
            .apply(lambda x: json.dumps(x))
        products_info['offer.supplyScheduleDays'] = products_info['offer.supplyScheduleDays'] \
            .apply(lambda x: json.dumps(x))
        products_info['offer.processingState.notes'] = products_info['offer.processingState.notes'] \
            .apply(lambda x: json.dumps(x))

        return products_info.reset_index().drop(columns='index')


def products_info_db_update(db_creds, table_name):

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


def products_info_to_db_daily(db_creds, table_name):

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


def get_skus_info() -> pd.DataFrame:
    """
    Отчеты > Отчет по товарам /v2/campaigns/{campaignId}/stats/skus.json
    """

    # info from products_info
    prod_info = get_products_info()
    five100s = int(prod_info.shape[0] / 500)

    # query

    url = f'https://api.partner.market.yandex.ru/v2/campaigns/{YA_CAMPAIGN_ID}/stats/skus.json'

    yandex_skus = pd.DataFrame()
    for k in range(five100s):
        sku_list = prod_info['offer.shopSku'].loc[0 + (500 * k):499 + (500 * k)].tolist()

        params = {
            'campaignId': YA_CAMPAIGN_ID,
            'shopSkus': sku_list
        }

        r = requests.post(url, headers=headers, json=params)
        r_dict = json.loads(r.text)
        r_data = r_dict['result']['shopSkus']
        df_temp = pd.json_normalize(r_data)

        yandex_skus = yandex_skus.append(df_temp)

    sku_list = prod_info['offer.shopSku'].loc[(500 * five100s):].tolist()
    params = {
        'campaignId': YA_CAMPAIGN_ID,
        'shopSkus': sku_list
    }

    r = requests.post(url, headers=headers, json=params)
    r_dict = json.loads(r.text)
    r_data = r_dict['result']['shopSkus']
    df_temp = pd.json_normalize(r_data)

    yandex_skus = yandex_skus.append(df_temp)

    yandex_skus.reset_index(inplace=True)
    yandex_skus.drop(columns=['index'], inplace=True)
    yandex_skus['warehouses'].fillna(0, inplace=True)

    yandex_skus['stock_plus_reserved'] = yandex_skus['warehouses'].apply(
        lambda x: x[0]['stocks'][0]['count'] if x != 0 else 0)

    for column in ['hidings', 'storage', 'warehouses', 'tariffs', 'pictures', 'hidingReasons']:
        if column in yandex_skus.columns:
            yandex_skus[column] = yandex_skus[column].apply(lambda x: json.dumps(x))

    return yandex_skus


def skus_info_db_update(db_creds, table_name):

    skus = get_skus_info()

    engine = create_engine(db_creds, echo=True)

    try:
        with engine.connect() as conn:
            stmt = text(f'DELETE FROM {table_name}')
            conn.execute(stmt)

        skus.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False
        )

    finally:
        engine.dispose()


def skus_info_to_db_daily(db_creds, table_name):

    skus = get_skus_info()
    skus['date'] = date.today()

    engine = create_engine(db_creds, echo=True)

    try:
        skus.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False
        )

    finally:
        engine.dispose()


def get_orders_fbs(day) -> pd.DataFrame:
    """
    Запросы магазина к маркету > Информация о заказах
    /v2/campaigns/{campaignId}/orders.json

    day: dd-mm-yyyy data format needed, just apply .strftime(format='%d-%m-%Y') to it
    """

    url = f'https://api.partner.market.yandex.ru/v2/campaigns/{YA_CAMPAIGN_ID}/orders.json'

    params = {
        'campaignId': YA_CAMPAIGN_ID,
        'fake': 'false',
        'fromDate': str(day),
        'toDate': str(day),
    }

    # Получение кол-ва страниц
    r = requests.get(url, params=params, headers=headers)
    num_of_pages = json.loads(r.text)['pager']['pagesCount']

    if num_of_pages == 0:
        return pd.DataFrame()

    # Загрузка данных
    orders = pd.DataFrame()
    for i in range(num_of_pages):
        params = {
            'campaignId': YA_CAMPAIGN_ID,
            'fake': 'false',
            'fromDate': str(day),
            'toDate': str(day),
            'page': str(i + 1)
        }

        r = requests.get(url, params=params, headers=headers)
        dict_yandex = json.loads(r.text)['orders']
        df_temp = pd.json_normalize(dict_yandex)
        orders = orders.append(df_temp)

    orders.reset_index(inplace=True)

    # удаление ненужных колонок и переименование нужных
    cols_to_drop = ['index',
                    'itemsTotal',
                    'total',
                    'buyerTotal',
                    'buyerItemsTotal',
                    'buyerTotalBeforeDiscount',
                    'buyerItemsTotalBeforeDiscount',
                    'deliveryTotal',
                    'subsidyTotal',
                    'totalWithSubsidy',
                    'feeUE',
                    'subsidies']

    for column in cols_to_drop:
        if column in orders:
            orders.drop(columns=column, inplace=True)

    orders.rename(columns={'id': 'order_id'}, inplace=True)

    # распаковка колонки delivery.tracks
    if 'delivery.tracks' in orders.columns:
        orders_tracks = pd.DataFrame()
        for i in range(orders.shape[0]):
            if orders['delivery.tracks'][i] is np.nan:
                continue
            row = pd.json_normalize(orders['delivery.tracks'][i])
            row['order_id'] = orders.order_id[i]
            orders_tracks = pd.concat([orders_tracks, row])

        orders = orders.merge(orders_tracks, how='left', on='order_id')
        orders.drop(columns=['delivery.tracks'], inplace=True)

    # распаковка колонки items
    orders_items = pd.DataFrame()
    for i in range(orders.shape[0]):
        row = pd.json_normalize(orders['items'][i])
        row['order_id'] = orders.order_id[i]
        orders_items = pd.concat([orders_items, row])

    orders_items.rename(columns={'id': 'item_id'}, inplace=True)
    orders = orders.merge(orders_items, how='right', on='order_id')
    orders.drop(columns=['items'], inplace=True)

    # финальные преобразования
    if 'delivery.shipments' in orders.columns:
        orders['delivery.shipments'] = list(map(lambda x: json.dumps(x), orders['delivery.shipments']))
    if 'promos' in orders.columns:
        orders['promos'] = list(map(lambda x: json.dumps(x), orders['promos']))
    if 'subsidies' in orders.columns:
        orders['subsidies'] = list(map(lambda x: json.dumps(x), orders['subsidies']))
    if 'instances' in orders.columns:
        orders['instances'] = list(map(lambda x: json.dumps(x), orders['instances']))
    if 'details' in orders.columns:
        orders['details'] = list(map(lambda x: json.dumps(x), orders['details']))

    if 'expiryDate' in orders.columns:
        orders['expiryDate'] = pd.to_datetime(orders['expiryDate'],format='%d-%m-%Y %H:%M:%S')
    if 'delivery.dates.realDeliveryDate' in orders.columns:
        orders['delivery.dates.realDeliveryDate'] = pd.to_datetime(orders['delivery.dates.realDeliveryDate'], format='%d-%m-%Y')

    orders['creationDate'] = pd.to_datetime(orders['creationDate'], format='%d-%m-%Y %H:%M:%S')
    orders['delivery.dates.fromDate'] = pd.to_datetime(orders['delivery.dates.fromDate'], format='%d-%m-%Y')
    orders['delivery.dates.toDate'] = pd.to_datetime(orders['delivery.dates.toDate'], format='%d-%m-%Y')

    return orders


def orders_fbs_to_db(days_ago, db_creds, table_name):

    # получение данных по заказам
    orders_fbs = pd.DataFrame()
    for i in range(days_ago + 1):
        day = date.today() - timedelta(days=i)
        orders_fbs_temp = get_orders_fbs(day.strftime(format='%d-%m-%Y'))
        orders_fbs = pd.concat([orders_fbs, orders_fbs_temp])

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
        orders_fbs.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False
        )

    finally:
        engine.dispose()
