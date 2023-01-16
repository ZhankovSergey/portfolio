from clickhouse_driver import Client


def execute_query(query, db_creds):
    """
    db_ip указывается без http/https и без порта
    """

    with Client.from_url(db_creds) as client:
        client.execute(query)


