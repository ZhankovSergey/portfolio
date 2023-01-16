from typing import Text
from datetime import timedelta, datetime, date
import pandas as pd
import gspread
import numpy as np

from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Text, DateTime, Float, Date


def get_products_info_1c(google_creds_path, spreadsheet_id):

    gc = gspread.service_account(filename=google_creds_path)
    sheet = gc.open_by_key(spreadsheet_id)
    worksheet = sheet.get_worksheet(0)
    data = worksheet.get_values()
    headers = data.pop(0)
    products_info_1c = pd.DataFrame(data, columns=headers)

    products_info_1c['vendor_code'] = products_info_1c['vendor_code'].astype('str')

    products_info_1c['isna'] = products_info_1c['characteristic'].apply(lambda x: 1 if x is np.nan else 0)
    products_info_1c_is_not_na = products_info_1c.query('isna == 0')
    products_info_1c_is_na = products_info_1c.query('isna == 1')

    products_info_1c_is_not_na['name'] = products_info_1c_is_not_na['item_name'] + ' ' + products_info_1c_is_not_na[
        'characteristic']
    products_info_1c_is_na['name'] = products_info_1c_is_na['item_name']

    products_info_1c = pd.concat([products_info_1c_is_not_na, products_info_1c_is_na])

    products_info_1c.drop_duplicates(subset=['vendor_code'], inplace=True)
    products_info_1c.reset_index(inplace=True)
    products_info_1c.drop(columns=['isna', 'index'], inplace=True)

    return products_info_1c


def products_info_1c_postgre_update(google_creds_path, spreadsheet_id, table_name, engine_creds):

    prod_info = get_products_info_1c(google_creds_path, spreadsheet_id)

    engine = create_engine(engine_creds)

    try:
        with engine.connect() as conn:
            stmt = text(f'DELETE FROM {table_name}')
            conn.execute(stmt)

        prod_info.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False,
            dtype={
                'vendor_code': Text,
                'barcode': Text,
                'item_name': Text,
                'name': Text,
                'brand': Text,
                'characteristic': Text,
                'cell': Text
            }
        )

    except Exception as err:
        print(err)

    finally:
        engine.dispose()
