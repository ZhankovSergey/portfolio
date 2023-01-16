import os
import contextlib
from PyPDF2 import PdfFileMerger
from pathlib import Path
import pandas as pd
import telebot
import requests
import resource
from sqlalchemy import create_engine
import time

from os import getenv
from dotenv import load_dotenv
load_dotenv()
PG_BF_USER_ENGINE = getenv('PG_BF_USER_ENGINE')
TELE_TOKEN_OZON_BOT = getenv('TELE_TOKEN_OZON_BOT')
OZON_API_CID = getenv('OZON_API_CID')
OZON_API_KEY = getenv('OZON_API_KEY')

bot = telebot.TeleBot(TELE_TOKEN_OZON_BOT)
engine = create_engine(PG_BF_USER_ENGINE, echo=True)


@bot.message_handler(content_types=['document'])
def docs_functions(message):

    resource.setrlimit(resource.RLIMIT_NOFILE, (65536, 65536))  # increase the limit of open files at one time in ubuntu

    # Генерация стикеров с таблицей по отправленному в чат csv файлу из лк озона
    if message.document.file_name == 'ozon.csv':

        # получение и сохранение таблицы из чата
        file_info = bot.get_file(message.document.file_id)
        csv_file = requests.get(f'https://api.telegram.org/file/bot{TELE_TOKEN_OZON_BOT}/{file_info.file_path}')
        with open('/home/analyst1/www/ozon_stickers/ozon_lk.csv', 'wb') as file:
            file.write(csv_file.content)

        bot.reply_to(message, f'Таблица создается...')

        # загрузка сохраненной таблицы с сервера
        input_csv = pd.read_csv('/home/analyst1/www/ozon_stickers/ozon_lk.csv', delimiter=';', dtype={'Артикул': str})
        input_csv.dropna(subset=['Номер заказа'], inplace=True)

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

        prod_info_1c = pd.read_sql(sql_query, engine)

        # удаление ненужных данных из озоновской таблицы и форматирование
        orders_fbs_postings = input_csv[['Номер отправления', 'Артикул', 'Количество']]
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
        orders_fbs_postings.to_excel('/home/analyst1/www/ozon_stickers/ozon_stickers_table.xlsx', index=False)

        bot.reply_to(message, f'Стикеры создаются...')

        # запрос в апи озона не генерацию стикеров по списку номеров отправлений
        posting_numbers_list = list(orders_fbs_postings.posting_number.drop_duplicates())

        headers = {
            'Host': 'api-seller.ozon.ru',
            'Client-Id': OZON_API_CID,
            'Api-Key': OZON_API_KEY,
            'Content-Type': 'application/json'
        }

        url = 'https://api-seller.ozon.ru/v2/posting/fbs/package-label'

        # генерация пдфок по каждому отправлению и их сохранение в папку
        postings_quantity = len(posting_numbers_list)
        for i in range(postings_quantity):

            if i == round(postings_quantity * 0.1):
                bot.reply_to(message, f'Готово 10%...')
            if i == round(postings_quantity * 0.2):
                bot.reply_to(message, f'Готово 20%...')
            if i == round(postings_quantity * 0.3):
                bot.reply_to(message, f'Готово 30%...')
            if i == round(postings_quantity * 0.4):
                bot.reply_to(message, f'Готово 40%...')
            if i == round(postings_quantity * 0.5):
                bot.reply_to(message, f'Готово 50%...')
            if i == round(postings_quantity * 0.6):
                bot.reply_to(message, f'Готово 60%...')
            if i == round(postings_quantity * 0.7):
                bot.reply_to(message, f'Готово 70%...')
            if i == round(postings_quantity * 0.8):
                bot.reply_to(message, f'Готово 80%...')
            if i == round(postings_quantity * 0.9):
                bot.reply_to(message, f'Готово 90%...')

            query = {"posting_number": [posting_numbers_list[i]]}

            r = requests.post(url, headers=headers, json=query)

            if 'INVALID_ARGUMENT' in r.text:
                continue

            while r.text.startswith('%PDF') is False:
                time.sleep(1)
                r = requests.post(url, headers=headers, json=query)

            file_bytes = r.content

            with open(f'/home/analyst1/www/ozon_stickers/temp/sticker-{i:04d}.pdf', 'wb') as file:
                file.write(file_bytes)

        # объединение первых страниц всех пдфок в одну и её сохранение
        list_of_pdfs = os.listdir('/home/analyst1/www/ozon_stickers/temp/')
        list_of_pdfs.sort()

        with contextlib.ExitStack() as stack:
            merger = PdfFileMerger()
            files = [stack.enter_context(open(f'/home/analyst1/www/ozon_stickers/temp/{pdf}', 'rb')) for pdf in list_of_pdfs]

            for f in files:
                merger.append(fileobj=f, pages=(0, 1))
            with open('/home/analyst1/www/ozon_stickers/ozon_stickers.pdf', 'wb') as f:
                merger.write(f)

        # очистка папки temp
        [f.unlink() for f in Path('/home/analyst1/www/ozon_stickers/temp/').glob('*') if f.is_file()]

        # отправка результата в чат
        with open('/home/analyst1/www/ozon_stickers/ozon_stickers_table.xlsx', 'rb') as doc_xlsx:
            bot.send_document(message.chat.id, doc_xlsx)

        with open('/home/analyst1/www/ozon_stickers/ozon_stickers.pdf', 'rb') as doc_pdf:
            bot.send_document(message.chat.id, doc_pdf)

    elif '.csv' not in message.document.file_name:
        bot.reply_to(message, 'Неверный формат файла. Допустимый формат: .csv.')
    else:
        bot.reply_to(message, 'Неверное имя файла, допустимые имена: ozon.csv')
        # bot.reply_to(message, '1) "ozon.csv" - для получения стикеров')
        # bot.reply_to(message, '2) "заказы" - для получения артикула товара по номеру заказа WB')


@bot.message_handler(commands=['status'])
def show_status(message):

    bot.reply_to(message, f'Бот онлайн')


@bot.message_handler(commands=['info'])
def show_status(message):

    bot.reply_to(message, 'Для получения стикеров нужно отправить таблицу csv из лк озона.\n '
                          'Название таблицы должно быть "ozon.csv"\n '
                          'В таблице должны быть колонки: "Номер отправления", "Артикул", "Количество"')


bot.infinity_polling()
