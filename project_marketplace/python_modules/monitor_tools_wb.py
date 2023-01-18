"""
Модуль для:
    1) парсинга товаров и информации по ним с wildberries.ru
    2) создания базы данных с мониторингом цен товаров конкурентов, соответствующих нашим товарам
"""
import requests
import json
import pandas as pd
import random
from bs4 import BeautifulSoup
import re
from time import sleep
import pickle
from datetime import date
from sqlalchemy import create_engine, text

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException


def get_product_list_from_cat_pages(category_url_list: list, chrome_driver_path: str, price_limit: int) -> pd.DataFrame:
    """
    Получение списка товаров по списку урлов категорий.

    :param category_url_list: список урлов категорий/подкатегорий вида https://www.wildberries.ru/catalog/category_name?sort=popular&page=1
    :param chrome_driver_path: путь до хромдрайвера
    :param price_limit: верхняя граница поиска товаров по цене (актуально для категорий, где больше 10 000 товаров
                        и все спарсить нельзя из-за ограничений WB.
                        Тогда парсится по диапазонам цен пока не достигнет price_limit.

    :return: датафрейм с колонками (id товара, ссылка на карточку товара, ссылка на джейсон карточки)
    """

    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--incognito')
    options.add_argument('--headless')
    options.add_argument('--disable-dev-shm-usage')
    browser = webdriver.Chrome(chrome_driver_path, chrome_options=options)

    products_list = pd.DataFrame()

    try:
        for category_url in category_url_list:
            # узнаем сколько товаров в категории
            browser.get(category_url)
            prod_cnt_elem = WebDriverWait(browser, 60).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, ".goods-count"))).text

            cat_prod_cnt = int(re.sub("[^0-9]", "", prod_cnt_elem))
            print(f'{category_url} {cat_prod_cnt} {prod_cnt_elem}')

            # если в категории больше 10 000 товаров, то они разбиваются с помощью фильтра по цене
            if cat_prod_cnt > 10000:
                min_price = 201
                max_price = 250

                while max_price <= price_limit:
                    page_url = category_url + f'&priceU={min_price}00%3B{max_price}00'

                    # получение кол-ва товаров по заданному фильтру
                    browser.get(page_url)
                    prod_cnt_elem = WebDriverWait(browser, 60).until(
                        EC.visibility_of_element_located((By.CSS_SELECTOR, ".goods-count"))).text

                    prod_cnt = int(re.sub("[^0-9]", "", prod_cnt_elem))

                    # получение датафрейма со списком товаров по заданному фильтру
                    current_page = 1

                    while current_page < 101:
                        print(browser.current_url)
                        for i in range(25):
                            ActionChains(browser).scroll_by_amount(1, 1000).perform()

                        html_data = BeautifulSoup(browser.page_source, 'html5lib')
                        product_cards = html_data.find_all(class_='product-card__wrapper')

                        for item in product_cards:
                            nm_id = item.img['src'].split('/')[5]
                            url = 'https://www.wildberries.ru/catalog/' + nm_id + '/detail.aspx'
                            json_src = 'https:' + item.img['src'].split('images')[0] + 'info/ru/card.json'
                            price_with_disc = int(re.sub("[^0-9]", "", item(attrs={'class': 'price__lower-price'})[0].text))

                            products_list = products_list.append(
                                pd.DataFrame([[nm_id, url, json_src, price_with_disc]],
                                             columns=['nm_id', 'url', 'json_src', 'price_with_disc']))

                        try:
                            next_page_link = browser.find_element(By.CLASS_NAME, 'pagination-next')
                            next_page_link.send_keys(Keys.ENTER)
                            sleep(random.randint(1, 3))
                        except NoSuchElementException:
                            break

                    # изменение фильтра
                    min_price = max_price + 1

                    if max_price < 500:
                        max_price += 50
                    elif max_price < 1000:
                        max_price += 100
                    elif max_price < 4000:
                        max_price += 200
                    else:
                        max_price += 1000

                print(f'{products_list.shape}')

            # если товаров в категории меньше 10 000, то просто парсится постранично
            else:
                current_page = 1

                while current_page <= cat_prod_cnt // 100 + 1:
                    print(browser.current_url)
                    for i in range(25):
                        ActionChains(browser).scroll_by_amount(1, 1000).perform()

                    html_data = BeautifulSoup(browser.page_source, 'html5lib')
                    product_cards = html_data.find_all(class_='product-card__wrapper')

                    for item in product_cards:
                        nm_id = item.img['src'].split('/')[5]
                        url = 'https://www.wildberries.ru/catalog/' + item.img['src'].split('/')[5] + '/detail.aspx'
                        json_src = 'https:' + item.img['src'].split('images')[0] + 'info/ru/card.json'
                        price_with_disc = int(re.sub("[^0-9]", "", item(attrs={'class': 'price__lower-price'})[0].text))

                        products_list = products_list.append(
                            pd.DataFrame([[nm_id, url, json_src, price_with_disc]],
                                         columns=['nm_id', 'url', 'json_src', 'price_with_disc']))

                    try:
                        next_page_link = browser.find_element(By.CLASS_NAME, 'pagination-next')
                        next_page_link.send_keys(Keys.ENTER)
                        current_page += 1
                        sleep(random.randint(1, 3))
                    except NoSuchElementException:
                        break

                print(f'{products_list.shape}')

            # итоговые преобразования
            products_list.drop_duplicates(inplace=True)
            products_list.reset_index(inplace=True)
            products_list.drop(columns='index', inplace=True)

    finally:
        if browser:
            browser.quit()

    return products_list


def save_product_list_from_cat_pages(chrome_driver_path: str,
                                     pkl_backup_path: str,
                                     category_url_list: list,
                                     price_limit: int):
    """
    Обновляет список урлов товаров для парсинга в базе данных и сохраняет на сервере в pkl.

    :param chrome_driver_path: путь до исполняемого файла хром драйвера
    :param pkl_backup_path: путь для сохранения данных в pkl
    :param category_url_list: список урлов категорий/подкатегорий вида https://www.wildberries.ru/catalog/category_name?sort=popular&page=1
    :param price_limit: верхняя граница поиска товаров по цене (актуально для категорий,
                        где больше 10 000 товаров и все спарсить нельзя из-за ограничений WB.
                        Тогда парсится частями - по диапазонам цен, пока не достигнет price_limit.
    """

    # получение данных
    wb_product_list = get_product_list_from_cat_pages(category_url_list, chrome_driver_path, price_limit)

    # сохранение в pkl
    with open(pkl_backup_path, 'wb') as file:
        pickle.dump(wb_product_list, file)


def parse_product_cards_data_json(prod_list: pd.DataFrame):
    """
    Функция для парсинга параметров товаров, которые можно достать из джейсона карточки на сайте WB.

    :param prod_list: датафрейм в котором есть 2 колонки: json_src - урл джейсона карточки, nm_id - id товара

    :return: исходный датафрейм обогащенный данными из джейсонов карточек товаров
    """

    prod_list['nm_id'] = prod_list.nm_id.astype('str')

    # получение данных описания товара из джейсона карточки
    products_cards_json_info = pd.DataFrame()
    for i in range(prod_list.shape[0]):
        print(f'{i} {prod_list.json_src[i]}')
        try:
            json_data = requests.get(prod_list.json_src[i]).text
        except Exception as error:
            print(error)
            continue

        if '404 Not Found' not in json_data:
            data = json.loads(json_data)
            products_cards_json_info_temp = pd.json_normalize(data)
            products_cards_json_info = pd.concat([products_cards_json_info, products_cards_json_info_temp])

    products_cards_json_info.reset_index(inplace=True)
    products_cards_json_info.drop(columns='index', inplace=True)

    print('распаковка options')
    options = pd.DataFrame()
    for i in range(products_cards_json_info.shape[0]):
        if type(products_cards_json_info.options[i]) is not float:
            options_temp = pd.json_normalize(products_cards_json_info['options'][i])
            options_temp = options_temp[['name', 'value']]
            options_temp['index'] = '1'
            options_temp.drop_duplicates(subset=['name'], inplace=True)
            options_temp = options_temp.pivot(columns='name', values='value', index='index')
            options_temp['nm_id'] = products_cards_json_info['nm_id'][i]
            options = pd.concat([options, options_temp])

    # мердж и дроп ненужных колонок
    products_cards_json_info.drop(columns=['grouped_options', 'options'], inplace=True)
    products_cards_json_info = products_cards_json_info.merge(options, on='nm_id', how='left')

    products_cards_json_info['nm_id'] = products_cards_json_info.nm_id.astype('str')
    products_cards_json_info['imt_id'] = products_cards_json_info.nm_id.astype('str')

    product_cards_info = prod_list.merge(products_cards_json_info, on='nm_id', how='left')

    return product_cards_info


def parse_product_cards_data(prod_list: pd.DataFrame,
                             chrome_driver_path: str,
                             param_price_with_disc=True,
                             param_seller_name=False,
                             param_json_src=False) -> pd.DataFrame:
    """
    Функция для парсинга параметров товаров, которые можно достать из карточки на сайте WB.

    :param prod_list: датафрейм в котором есть колонка url - урл карточки
    :param chrome_driver_path: путь до исполняемого файла хром драйвера
    :param param_price_with_disc: собирать или нет цену с карточки
    :param param_seller_name: собирать или нет название продавца
    :param param_json_src: собирать или нет адрес джейсона с данными о товаре

    :return: исходный датафрейм + параметры с карточки WB
    """

    # инициализация браузера
    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--incognito')
    options.add_argument('--headless')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(chrome_driver_path, chrome_options=options)

    if param_price_with_disc:
        prod_list['price_with_disc'] = 0
    if param_seller_name:
        prod_list['seller_name'] = ''
    if param_json_src:
        prod_list['json_src'] = ''

    try:
        for i in range(prod_list.shape[0]):
            driver.get(prod_list.url[i])
            sleep(2)
            html_data = BeautifulSoup(driver.page_source, 'html5lib')

            if param_price_with_disc:
                final_price_elem = html_data.find(class_='price-block__final-price')
                if final_price_elem is None:
                    prod_list.price_with_disc[i] = 0
                else:
                    prod_list.price_with_disc[i] = int(re.sub("[^0-9]", "", final_price_elem.text))

            if param_seller_name:
                seller_info_elem = html_data.find(class_='seller-info__name')
                if seller_info_elem is None:
                    prod_list.seller_name[i] = ''
                else:
                    prod_list.seller_name[i] = seller_info_elem.text

            if param_json_src:
                img_src = html_data.find(class_='slide__content img-plug j-wba-card-item').img['src']
                prod_list.json_src[i] = 'https:' + img_src.split('images')[0] + 'info/ru/card.json'

            print(f'{i} {prod_list.url[i]}')

    except Exception as error:
        print(error)

    finally:
        if driver:
            driver.quit()

    return prod_list


def save_prod_list_with_json_data(prod_list_pkl_path: str,
                                  wb_prod_list_data_pkl_save_path: str):
    """
    Парсит информацию из джейсона карточек товаров по списку и сохраняет на сервер.

    :param prod_list_pkl_path: путь до сохраненного в pkl датафрейма с товарами
    :param wb_prod_list_data_pkl_save_path: путь для сохранения данных в pkl
    """

    with open(prod_list_pkl_path, 'rb') as prod_list_pkl:
        prod_list = pickle.load(prod_list_pkl)

    prod_list_data = parse_product_cards_data_json(prod_list)

    with open(wb_prod_list_data_pkl_save_path, 'wb') as file:
        pickle.dump(prod_list_data, file)


def get_our_stock_data(chrome_driver_path: str,
                       db_creds: str) -> pd.DataFrame:
    """
    Парсинг данных с сайта WB о наших товарах на стоке.

    :param chrome_driver_path: путь до исполняемого файла хром драйвера
    :param db_creds: данные для инициализации движка базы вида 'postgresql+psycopg2://user:pass@host:port/db_name'

    :return: датафрейм с данными о наших товарах с карточек wb
    """

    # получение нашего стока из базы
    sql_query = '''
                    SELECT
                        "nmId" AS nm_id,
                        article AS article
                    FROM wb_products_info
                    WHERE stock > 0
                    UNION
                    SELECT
                        "nmId" AS nm_id,
                        "supplierArticle" AS article
                    FROM wb_stat_stocks
                    WHERE quantity > 0
                '''

    engine = create_engine(db_creds, echo=True)
    try:
        our_stock = pd.read_sql(sql_query, con=engine)
    finally:
        engine.dispose()

    our_stock['url'] = 'https://www.wildberries.ru/catalog/' + our_stock.nm_id + '/detail.aspx'

    # обогащение данными с карточки
    our_stock = parse_product_cards_data(our_stock, chrome_driver_path, param_json_src=True)

    # обогащение данными из джейсона карточки
    our_stock = parse_product_cards_data_json(our_stock)

    return our_stock


def save_our_stock_data(db_creds: str,
                        chrome_driver_path: str,
                        pkl_backup_path: str):
    """
    Сохраняет актуальную информацию с сайта wb о наших товарах на стоке.

    :param db_creds: данные для инициализации движка базы вида 'postgresql+psycopg2://user:pass@host:port/db_name'
    :param chrome_driver_path: путь до исполняемого файла хром драйвера
    :param pkl_backup_path: путь для сохранения датафрейма в pkl
    """

    our_stock = get_our_stock_data(chrome_driver_path, db_creds)

    with open(pkl_backup_path, 'wb') as file:
        pickle.dump(our_stock, file)


def prices_update(engine_creds, chrome_driver_path):
    """
    Обновляет цены конкурентов в таблице мониторинга
    """

    # четыре раза в месяц обновляет все товары,
    # в остальные дни только товары, которые были в наличии на время последней проверки
    if date.today().day in (7, 14, 21, 28):
        where_condition = "mp='WB'"
    else:
        where_condition = "mp='WB' AND price_with_disc != 0"

    db_engine = create_engine(engine_creds)

    try:
        # загрузка списка товаров wb в текущем списке мониторинга
        sql_query = f'''
                        SELECT id, url
                        FROM monitor_mp_prices
                        WHERE {where_condition}
                     '''

        prod_list = pd.read_sql(sql_query, con=db_engine)

        prod_list_cards_data = parse_product_cards_data(prod_list, chrome_driver_path)

        # обновление price_with_disc в бд
        for i in range(prod_list_cards_data.shape[0]):
            with db_engine.connect() as conn:
                stmt = text(f'''
                                UPDATE monitor_mp_prices
                                SET price_with_disc = {prod_list_cards_data.price_with_disc[i]}
                                WHERE mp = 'WB' AND id = '{prod_list_cards_data.id[i]}'
                            ''')
                conn.execute(stmt)

    finally:
        db_engine.dispose()


def our_prices_update(engine_creds, chrome_driver_path):
    """
    Обновляет наши цены в таблице мониторинга
    """

    db_engine = create_engine(engine_creds)

    try:
        # загрузка списка наших товаров в мониторинге wb
        sql_query = '''
                        SELECT
                            DISTINCT(our_id) AS id,
                            our_url AS url
                        FROM monitor_mp_prices
                        WHERE mp='WB'
                    '''

        prod_list = pd.read_sql(sql_query, con=db_engine)
        prod_list_cards_data = parse_product_cards_data(prod_list, chrome_driver_path)

        # обновление our_price
        for i in range(prod_list_cards_data.shape[0]):
            with db_engine.connect() as conn:
                stmt = text(f'''
                                UPDATE monitor_mp_prices
                                SET our_price = {prod_list_cards_data.price_with_disc[i]}
                                WHERE mp = 'WB' AND our_id = '{prod_list_cards_data.id[i]}'
                            ''')
                conn.execute(stmt)

    finally:
        db_engine.dispose()
