### [materialized_views_update.py](materialized_views_update.py)
Обновления материализованных представлений в постгресе.
### [monitoring_parse_data.py](monitoring_parse_data.py)
Парсинг данных с сайта маркетплейса.
### [monitoring_parse_data_from_other_sites.py](monitoring_parse_data_from_other_sites.py)
Парсинг данных с других сайтов.
### [monitoring_update_prices.py](monitoring_update_prices.py)
Обновление в базе данных информации о ценах из апи маркетплейсов.
### [mp_orders_to_db.py](mp_orders_to_db.py)
Загрузка данных о заказах по апи маркетплейсов в базу.
### [mp_products_info_db_partial_update.py](mp_products_info_db_partial_update.py)
В течение дня несколько раз проверяет остатки и оставляет в базе самое высокое значение.
Нужно чтобы получить более точные исторические данные о наличии товаров. 
Так как товар может появиться в разное время в течение дня.
### [mp_products_info_to_db.py](mp_products_info_to_db.py)
Загрузка информации о товарах из отчетов по апи маркетплейсов, в том числе наличие.
### [mp_products_info_to_db_daily.py](mp_products_info_to_db_daily.py)
Сохранение исторических данных по дням.
### [mp_realization_to_db.py](mp_realization_to_db.py)
Загрузка отчета по реализации по апи.
### [mp_sales_to_db.py](mp_sales_to_db.py)
Загрузка данных о продажах по апи маркетплейсов в базу.
### [ozon_analitycs_data_to_db.py](ozon_analitycs_data_to_db.py)
Загрузка аналитических данных Озона по апи в базу.