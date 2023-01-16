import pandas as pd
from bs4 import BeautifulSoup
import re
from seleniumwire import webdriver
from time import sleep


def iherb_parse_catalog(url_list: list, chrome_driver_path: str, excel_save_path: str, chrome_proxy: str):
    """
    Парсит страницы каталога по заданным урлам страниц брендов / категорий и сохраняет в .xlsx
    """

    seleniumwire_options = {
        'proxy': {
            'http': f'http://{chrome_proxy}',
            'https': f'https://{chrome_proxy}',
            'no_proxy': 'localhost,127.0.0.1'
        }
    }

    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--incognito')
    options.add_argument('--headless')

    driver = webdriver.Chrome(chrome_driver_path, options=options, seleniumwire_options=seleniumwire_options)

    products_list = pd.DataFrame()

    for url in url_list:
        # получение количества товаров для вычисления количества страниц
        driver.get(url)
        sleep(2)

        html_data = BeautifulSoup(driver.page_source, 'html5lib')
        products_count_element = html_data.find(attrs={'class': "sub-header-title display-items"})
        while products_count_element is None:
            sleep(1)
            html_data = BeautifulSoup(driver.page_source, 'html5lib')
            products_count_element = html_data.find(attrs={'class': "sub-header-title display-items"})

        cat_products_count = re.sub("\(.*", "", products_count_element.text)
        cat_products_count = int(re.sub("[^0-9]", "", cat_products_count))

        # парсинг со всех страниц панинации
        current_page = 1

        while current_page <= cat_products_count // 24 + 1:
            print(f'Парсинг {url}?p={current_page}')
            driver.get(f'{url}?p={current_page}')
            sleep(1)
            page_source = driver.page_source
            html_data = BeautifulSoup(page_source, 'html5lib')
            product_cards_list = html_data.find_all(class_='product-cell-container')

            for product_card in product_cards_list:
                card_url = product_card.find(class_='absolute-link-wrapper').a['href']
                name = product_card.find(class_='absolute-link-wrapper').a['title']
                brand = product_card.find(class_='absolute-link-wrapper').a['data-ga-brand-name']
                product_code = product_card.find(class_='absolute-link-wrapper').a['data-part-number']
                price_with_disc = float(re.sub("[^0-9.]", "", product_card.find(class_='absolute-link-wrapper').a['data-ga-discount-price']))

                out_of_stock_status = product_card.find(class_='absolute-link-wrapper').a['data-ga-is-out-of-stock']
                if out_of_stock_status == 'False':
                    out_of_stock = 'В наличии'
                else:
                    out_of_stock = 'Нет в наличии'

                if product_card.find(class_='price-olp') is not None:
                    price_without_disc = float(re.sub("[^0-9.]", "", product_card.find(class_='price-olp').bdi.text))
                else:
                    price_without_disc = price_with_disc

                if product_card.find(class_='discount-in-cart') is not None:
                    discount_in_cart = product_card.find(class_='discount-in-cart')['title']
                else:
                    discount_in_cart = ''

                discount = round(abs(price_with_disc / price_without_disc - 1), 1)

                print(f'Добавление строки: {card_url}, {name}, {brand}, {product_code}, {price_with_disc}, '
                      f'{price_without_disc}, {discount}, {out_of_stock}, {discount_in_cart}')

                products_list = products_list.append(
                    pd.DataFrame([[card_url, name, brand, product_code, price_with_disc, price_without_disc, discount,
                                   out_of_stock, discount_in_cart]],
                                 columns=['url', 'Наименование', 'Бренд', 'Код', 'Цена со скидкой', 'Цена без скидки',
                                          'Скидка', 'Наличие', 'Скидка в корзине']))

            current_page += 1
            sleep(1)

    products_list.reset_index(inplace=True)
    products_list.drop(columns='index', inplace=True)
    products_list.drop_duplicates(inplace=True)

    driver.quit()

    products_list.to_excel(excel_save_path, index=False)
