import os
import time
import telebot
import pandas as pd
import requests
import base64
from pathlib import Path
import resource
from datetime import date, timedelta
from PyPDF2 import PdfFileMerger
from svglib.svglib import svg2rlg
from reportlab.graphics import renderPDF

from os import getenv
from dotenv import load_dotenv
load_dotenv()
MODULES_DIR = getenv('MODULES_DIR')
TELE_TOKEN_WB_BOT = getenv('TELE_TOKEN_WB_BOT')
WB_API_KEY = getenv('MODULES_DIR')

import sys
sys.path.append(MODULES_DIR)
import api_tools_wb

bot = telebot.TeleBot(TELE_TOKEN_WB_BOT)


@bot.message_handler(content_types=['document'])
def docs_functions(message):

    resource.setrlimit(resource.RLIMIT_NOFILE, (65536, 65536))  # increase the limit of open files at one time in ubuntu

    # 1) Генерация стикеров и сопутствующей эксель таблицы
    if message.document.file_name == 'Таблица МП.xlsx':
        # Получение и предобработка файла с номерами заказов

        file_info = bot.get_file(message.document.file_id)
        excel_file = requests.get(f'https://api.telegram.org/file/bot{TELE_TOKEN_WB_BOT}/{file_info.file_path}')

        wh_data = pd.read_excel(excel_file.content)
        wh_data.rename(columns={wh_data.columns[1]: 'Заказ'}, inplace=True)
        wh_data.dropna(subset=['Заказ'], inplace=True)
        wh_data = wh_data[['Заказ']]
        wh_data = wh_data.astype({'Заказ': 'int'})

        order_numbers = list(wh_data['Заказ'])

        bot.reply_to(message, f'Стикеры создаются...')

        # Генерация pdf с qr-кодами
        headers = {
            'accept': 'application/json',
            'Authorization': WB_API_KEY,
            'Content-Type': 'application/json'
        }

        url = 'https://suppliers-api.wildberries.ru/api/v3/orders/stickers'

        params = {
            'type': 'svg',
            'width': 58,
            'height': 40,
        }

        for i in range(len(order_numbers)):

            if i == len(order_numbers) // 4:
                bot.reply_to(message, f'Готово 25%...')
            if i == len(order_numbers) // 2:
                bot.reply_to(message, f'Готово 50%...')
            if i == len(order_numbers) // 1.33:
                bot.reply_to(message, f'Готово 75%...')

            json = {
                'orders': [
                    order_numbers[i]
                ],
            }

            r = requests.post(url, headers=headers, json=json, params=params)

            if r.json()['stickers'] != []:
                # генерация svg
                r_json = r.json()['stickers'][0]
                file_bytes = base64.b64decode(r_json['file'], validate=True)

                with open("/home/analyst1/www/wb_stickers/temp/svg/{:04d}.svg".format(i), "wb") as file:
                    file.write(file_bytes)

                path_to_render_svg = Path('/home/analyst1/www/wb_stickers/temp/svg/{:04d}.svg'.format(i))

                # генерация pdf из svg
                drawing = svg2rlg(path_to_render_svg)
                renderPDF.drawToFile(drawing, '/home/analyst1/www/wb_stickers/temp/pdf/{:04d}.pdf'.format(i))

        # склейка всех pdf в один
        list_of_pdfs = os.listdir('/home/analyst1/www/wb_stickers/temp/pdf')
        list_of_pdfs.sort()

        merger = PdfFileMerger()
        for pdf in list_of_pdfs:
            merger.append(f'/home/analyst1/www/wb_stickers/temp/pdf/{pdf}')
        merger.write(f"/home/analyst1/www/wb_stickers/wb_stickers.pdf")
        merger.close()

        with open('/home/analyst1/www/wb_stickers/wb_stickers.pdf', 'rb') as doc:
            bot.send_document(message.chat.id, doc)

        [f.unlink() for f in Path('/home/analyst1/www/wb_stickers/temp/svg/').glob('*') if f.is_file()]
        [f.unlink() for f in Path('/home/analyst1/www/wb_stickers/temp/pdf/').glob('*') if f.is_file()]

        # Генерация эксельки с кодом штрихкода
        bot.reply_to(message, f'Таблица создается...')

        df_for_excel = pd.DataFrame()
        thousands = int(wh_data.shape[0] / 1000)

        headers = {
            'accept': 'application/json',
            'Authorization': WB_API_KEY,
            'Content-Type': 'application/json'
        }

        url = 'https://suppliers-api.wildberries.ru/api/v2/orders/stickers'

        if wh_data['Заказ'].shape[0] > 1000:
            for k in range(thousands):
                wh_data_temp = wh_data.loc[0 + (1000 * k):999 + (1000 * k)]
                order_numbers = str(list(wh_data_temp['Заказ']))

                jsonn = f'"orderIds":{order_numbers}'
                data = "{" + jsonn + "}"

                r = requests.post(url, headers=headers, data=data)
                data = r.json()['data']

                while data is None:
                    time.sleep(1)
                    r = requests.post(url, headers=headers, data=data)
                    data = r.json()['data']

                data = pd.json_normalize(data)
                data = data[['orderId', 'sticker.wbStickerId', 'sticker.wbStickerEncoded']]

                df_for_excel = pd.concat([df_for_excel, data])

        wh_data_temp = wh_data.loc[0 + (1000 * thousands):]
        order_numbers = str(list(wh_data_temp['Заказ']))

        jsonn = f'"orderIds":{order_numbers}'
        data = "{" + jsonn + "}"

        r = requests.post(url, headers=headers, data=data)
        data = r.json()['data']

        while data is None:
            time.sleep(1)
            r = requests.post(url, headers=headers, data=data)
            data = r.json()['data']

        data = pd.json_normalize(data)
        data = data[['orderId', 'sticker.wbStickerId', 'sticker.wbStickerEncoded']]

        df_for_excel = pd.concat([df_for_excel, data])

        df_for_excel.to_excel('/home/analyst1/www/wb_stickers/wb_stickers_excel.xlsx')

        with open('/home/analyst1/www/wb_stickers/wb_stickers_excel.xlsx', 'rb') as doc:
            bot.send_document(message.chat.id, doc)

    # 1.5) Старый вариант генерации до конца января
    elif message.document.file_name == 'old.xlsx':
        # Получение и предобработка файла с номерами заказов

        file_info = bot.get_file(message.document.file_id)
        excel_file = requests.get(f'https://api.telegram.org/file/bot{TELE_TOKEN_WB_BOT}/{file_info.file_path}')

        wh_data = pd.read_excel(excel_file.content)
        wh_data.rename(columns={wh_data.columns[1]: 'Заказ'}, inplace=True)
        wh_data.dropna(subset=['Заказ'], inplace=True)
        wh_data = wh_data[['Заказ']]
        wh_data = wh_data.astype({'Заказ': 'int'})

        order_numbers = list(wh_data['Заказ'])
        print(f'Всего заказов {len(order_numbers)}')

        # Генерация pdf с qr-кодами

        headers = {
            'accept': 'application/json',
            'Authorization': WB_API_KEY,
            'Content-Type': 'application/json'
        }

        url = 'https://suppliers-api.wildberries.ru/api/v2/orders/stickers/pdf'

        bot.reply_to(message, f'Стикеры создаются...')

        for i in range(len(order_numbers)):

            if i == len(order_numbers) // 4:
                bot.reply_to(message, f'Готово 25%...')
            if i == len(order_numbers) // 2:
                bot.reply_to(message, f'Готово 50%...')
            if i == len(order_numbers) // 1.33:
                bot.reply_to(message, f'Готово 75%...')

            query = "{" + f'"orderIds": [{order_numbers[i]}], "type" : "qr"' + "}"
            r = requests.post(url, headers=headers, data=query)

            while 'connect error' in r.text:
                time.sleep(2)
                r = requests.post(url, headers=headers, data=query)

            while r.json()['error'] is True:
                print(r.json()['errorText'])
                time.sleep(2)
                r = requests.post(url, headers=headers, data=query)

            b64 = r.json()['data']['file']
            b64bytes = base64.b64decode(b64, validate=True)

            with open("/home/analyst1/www/wb_stickers/temp/pdf/page_{:04d}.pdf".format(i), "wb") as file:
                file.write(b64bytes)

        list_of_pdfs = os.listdir('/home/analyst1/www/wb_stickers/temp/pdf')
        list_of_pdfs.sort()

        merger = PdfFileMerger()
        for pdf in list_of_pdfs:
            merger.append(f'/home/analyst1/www/wb_stickers/temp/pdf/{pdf}')
        merger.write(f"/home/analyst1/www/wb_stickers/wb_stickers.pdf")
        merger.close()

        [f.unlink() for f in Path('/home/analyst1/www/wb_stickers/temp/pdf/').glob('*') if f.is_file()]

        with open('/home/analyst1/www/wb_stickers/wb_stickers.pdf', 'rb') as doc:
            bot.send_document(message.chat.id, doc)

            # Генерация эксельки с кодом штрихкода
        bot.reply_to(message, f'Таблица создается...')

        df_for_excel = pd.DataFrame()
        thousands = int(wh_data.shape[0] / 1000)

        headers = {
            'accept': 'application/json',
            'Authorization': WB_API_KEY,
            'Content-Type': 'application/json'
        }

        url = 'https://suppliers-api.wildberries.ru/api/v2/orders/stickers'

        if wh_data['Заказ'].shape[0] > 1000:
            for k in range(thousands):
                wh_data_temp = wh_data.loc[0 + (1000 * k):999 + (1000 * k)]
                order_numbers = str(list(wh_data_temp['Заказ']))

                jsonn = f'"orderIds":{order_numbers}'
                data = "{" + jsonn + "}"

                r = requests.post(url, headers=headers, data=data)
                data = r.json()['data']

                while data is None:
                    time.sleep(1)
                    r = requests.post(url, headers=headers, data=data)
                    data = r.json()['data']

                data = pd.json_normalize(data)
                data = data[['orderId', 'sticker.wbStickerId', 'sticker.wbStickerEncoded']]

                df_for_excel = pd.concat([df_for_excel, data])

        wh_data_temp = wh_data.loc[0 + (1000 * thousands):]
        order_numbers = str(list(wh_data_temp['Заказ']))

        jsonn = f'"orderIds":{order_numbers}'
        data = "{" + jsonn + "}"

        r = requests.post(url, headers=headers, data=data)
        data = r.json()['data']

        while data is None:
            time.sleep(1)
            r = requests.post(url, headers=headers, data=data)
            data = r.json()['data']

        data = pd.json_normalize(data)
        data = data[['orderId', 'sticker.wbStickerId', 'sticker.wbStickerEncoded']]

        df_for_excel = pd.concat([df_for_excel, data])

        df_for_excel.to_excel('/home/analyst1/www/wb_stickers/wb_stickers_excel.xlsx')

        with open('/home/analyst1/www/wb_stickers/wb_stickers_excel.xlsx', 'rb') as doc:
            bot.send_document(message.chat.id, doc)

    # 2) Заполнение артикулов по id заказа wb
    elif message.document.file_name == 'заказы.xlsx':
        # получение файла
        file_info = bot.get_file(message.document.file_id)
        excel_file = requests.get(f'https://api.telegram.org/file/bot{TELE_TOKEN_WB_BOT}/{file_info.file_path}')
        orders_xlsx = pd.read_excel(excel_file.content, parse_dates=[0], dtype={'ID Заказа': str})

        # определение диапазона дат
        min_date = orders_xlsx.iloc(axis=1)[0].min().date() - timedelta(days=5)
        days_range = (date.today() - min_date).days

        # получение информации о заказах по нужному диапазону
        bot.reply_to(message, f'Получение информации о заказах...')
        orders = pd.DataFrame()
        for i in range(days_range + 1):
            day = min_date + timedelta(days=i)
            orders_temp = api_tools_wb.get_orders_fbs(day)
            orders = pd.concat([orders, orders_temp])

            if i == (days_range // 2):
                bot.reply_to(message, 'Готово 50%...')

        # склейка в итоговый файл
        orders_xlsx = orders_xlsx.merge(orders[['article', 'id']], how='left', left_on='ID Заказа',
                                        right_on='id')
        orders_xlsx = orders_xlsx[['Дата создания', 'ID Заказа', 'article']]
        orders_xlsx.rename(columns={'article': 'ID Товара'}, inplace=True)

        # локальное сохранение и отправка файла в телеграм
        orders_xlsx.to_excel('/home/analyst1/www/reports/orders_articles.xlsx', index=False)

        with open('/home/analyst1/www/reports/orders_articles.xlsx', 'rb') as doc:
            bot.send_document(message.chat.id, doc)

    elif '.xlsx' not in message.document.file_name:
        bot.reply_to(message, 'Неверный формат файла. Допустимый формат: .xlsx.')
    else:
        bot.reply_to(message, 'Неверное имя файла, допустимые имена:')
        bot.reply_to(message, '1) "Таблица МП" - для получения стикеров')
        bot.reply_to(message, '2) "заказы" - для получения артикула товара по номеру заказа WB (должны быть две колонки: Дата создания, ID Заказа)')


@bot.message_handler(commands=['status'])
def show_status(message):

    bot.reply_to(message, f'Бот онлайн')


bot.infinity_polling()
