from datetime import date
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.pool import NullPool
import gspread


def update_mat_view(view_name, db_creds):

    engine = create_engine(db_creds, echo=True)

    with engine.connect() as conn:
        stmt = text(f'REFRESH MATERIALIZED VIEW {view_name};')
        conn.execute(stmt)

    engine.dispose()


def update_stock_if_bigger(*func_args,
                           func,
                           db_creds: str,
                           db_table: str,
                           db_date_col_name: str,
                           stocks_col_name: str,
                           article_col_name: str):

    """
    Заменяет значение остатка, если оно больше записанного в базе.
    """

    stocks = func(*func_args)
    stocks = stocks[stocks[stocks_col_name] > 0]
    stocks.reset_index(inplace=True)

    engine = create_engine(db_creds)
    try:
        for i in range(stocks.shape[0]):
            with engine.connect() as conn:
                stmt = text(f'''
                                UPDATE {db_table}
                                SET "{stocks_col_name}" = {stocks[stocks_col_name][i]}
                                WHERE "{db_date_col_name}"::date = '{str(date.today())}'
                                  AND "{article_col_name}" = '{stocks[article_col_name][i]}'
                                  AND "{stocks_col_name}" < {stocks[stocks_col_name][i]}
                            ''')
                conn.execute(stmt)

    finally:
        engine.dispose()


def update_wrong_articles(engine_creds: str,
                             db_table: str,
                             db_table_article_col_name: str,
                             google_sheet_id: str,
                             google_sheet_worksheet_num: int,
                             google_token_path: str):

    """
    Заменяет неверные артикулы, при условии, что есть гугл таблица с колонками неверное значение-верное.

    :param engine_creds: строка с настройками движка базы
    :param db_table: название таблицы в базе
    :param db_table_article_col_name: название колонки, где будет заменяться артикул
    :param google_sheet_id: id гугл таблицы из которой будет браться информация о нужных артикулах
    :param google_sheet_worksheet_num: номер листа гугл таблицы (начинается с 0)
    :param google_token_path: токен для работы с гугл таблицами
    """

    engine = create_engine(engine_creds, poolclass=NullPool)

    try:
        gc = gspread.service_account(filename=google_token_path)
        sheet = gc.open_by_key(google_sheet_id)
        worksheet = sheet.get_worksheet(google_sheet_worksheet_num)
        data = worksheet.get_all_values()

        for row in data:
            with engine.connect() as conn:
                stmt = text(f'''
                        UPDATE {db_table}
                        SET "{db_table_article_col_name}" = '{row[1]}'
                        WHERE "{db_table_article_col_name}" = '{row[0]}'
                            ''')
                conn.execute(stmt)

    finally:
        engine.dispose()


def update_wrong_articles_with_2nd_condition(engine_creds: str,
                                             db_table: str,
                                             db_table_article_col_name: str,
                                             second_col_name: str,
                                             google_sheet_id: str,
                                             google_sheet_worksheet_num: int,
                                             google_token_path: str):

    """
    Заменяет неверные артикулы, при условии, что есть гугл таблица с колонками 'неверное_значение-верное'.
    Дополнительно учитывает еще одну колонку с условием для сопоставления (например размер).

    :param engine_creds: строка с настройками движка базы
    :param db_table: название таблицы в базе
    :param db_table_article_col_name: название колонки, где будет заменяться артикул
    :param second_col_name: название колонки с дополнительным условием сопоставления
    :param google_sheet_id: id гугл таблицы из которой будет браться информация о нужных артикулах
    :param google_sheet_worksheet_num: номер листа гугл таблицы (начинается с 0)
    :param google_token_path: токен для работы с гугл таблицами
    """

    engine = create_engine(engine_creds, poolclass=NullPool)

    try:
        gc = gspread.service_account(filename=google_token_path)
        sheet = gc.open_by_key(google_sheet_id)
        worksheet = sheet.get_worksheet(google_sheet_worksheet_num)
        data = worksheet.get_all_values()

        for row in data:
            with engine.connect() as conn:
                stmt = text(f'''
                        UPDATE {db_table}
                        SET "{db_table_article_col_name}" = '{row[1]}'
                        WHERE "{db_table_article_col_name}" = '{row[0]}' AND "{second_col_name}" = '{row[2]}'
                            ''')
                conn.execute(stmt)

    finally:
        engine.dispose()
