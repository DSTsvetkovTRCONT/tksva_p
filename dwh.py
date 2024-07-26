"""
Модуль содержит все функции относящиеся к взаимодействию с DWH
"""

from clickhouse_driver import Client
import logging
import psycopg2
import os
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

if not os.path.exists('logs'):
    os.mkdir('logs')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")

file_handler = RotatingFileHandler('logs/tksva_p.log', maxBytes=20480, backupCount=10)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

basedir = os.path.abspath(os.path.dirname(__file__))

if os.path.exists('.env'):
    load_dotenv(os.path.join(basedir, '.env'))
else:
    load_dotenv(os.path.join(basedir, '.envexample'))


def update_dwh_table_info():
    """
    Функция вносит в таблицу БД postgresql dwh_tables_info актуальную информацию об таблице DWH audit._sales__execution_orders,
    в том числе о списке станций и дорог, для формы веб-интерфейса tksva
    """

    logger.info('Обновляем таблицу dwh_tables_info.')
    try:
        sql = """SELECT
                    count() AS rows_qty,
                    min(`SVOD.Дата отправки`) AS min_date,
                    max(`SVOD.Дата отправки`) AS max_date,
                    replace(
                        arrayStringConcat(
                            arraySort(
                                groupArray(DISTINCT `Дор. Отправления`)
                                ),
                            '|'),
                        '"','') AS railway_names,
                    replace(
                        arrayStringConcat(
                            arraySort(
                                groupArray(DISTINCT `Ст. Отправления`)
                                ),
                            '|'),
                        '"','') AS station_names
                FROM
                    audit._sales__execution_orders"""

        dwh_connection = Client(host=os.environ.get('CLICK_HOST'),
                                port=os.environ.get('CLICK_PORT'),
                                database=os.environ.get('CLICK_DBNAME'),
                                user=os.environ.get('CLICK_USER'),
                                password=os.environ.get('CLICK_PWD'),
                                secure=True, verify=False)

        with dwh_connection:
            dwh_table_info = dwh_connection.execute(sql)
            dwh_table_info_dict = {"rows_qty": dwh_table_info[0][0],
                                   "min_date": dwh_table_info[0][1].strftime('%Y-%m-%d %H:%M:%S'),
                                   "max_date": dwh_table_info[0][2].strftime('%Y-%m-%d %H:%M:%S'),
                                   "railway_names": dwh_table_info[0][3],
                                   "station_names": dwh_table_info[0][4]}

            dwh_table_info_dict = str(dwh_table_info_dict).replace("'", '"')

        psql_connection = psycopg2.connect(host=os.environ.get('PSQL_HOST'),
                                           port=os.environ.get('PSQL_PORT'),
                                           database=os.environ.get('PSQL_DBNAME'),
                                           user=os.environ.get('PSQL_USER'),
                                           password=os.environ.get('PSQL_PWD'))
        with psql_connection:
            psql_connection.cursor().execute(f"""UPDATE
                                                dwh_tables_info
                                            SET
                                                table_info='{dwh_table_info_dict}',
                                                timestamp=now(),
                                                wants_to_refresh=False,
                                                wants_to_refresh_timestamp=now()
                                            WHERE
                                                dwh_table_name='audit._sales__execution_orders'""")
    except Exception:
        logging.exception('Обновляем таблицу dwh_tables_info.')


def sales_execution_orders_source_info():
    """
    Получаем информацию о состоянии таблицы DWH audit.sales__execution_orders
    """
    logger.info('Получаем информацию о состоянии таблицы'
                'DWH audit.sales__execution_orders')
    try:
        sql = """SELECT
                    count() AS rows_qty,
                    min(datetime_move_started) AS min_date,
                    max(datetime_move_started) AS max_date,
                    replace(
                        arrayStringConcat(
                            arraySort(
                                groupArray(DISTINCT railway_from_name)
                            ),
                            '|'),
                        '"',
                        '') AS railway_names,
                    replace(
                        arrayStringConcat(
                            arraySort(
                                groupArray(DISTINCT station_from_name)
                                ),
                            '|'),
                        '"','') AS station_names
                FROM
                    audit.sales__execution_orders"""

        dwh_connection = Client(host=os.environ.get('CLICK_HOST'),
                                port=os.environ.get('CLICK_PORT'),
                                database=os.environ.get('CLICK_DBNAME'),
                                user=os.environ.get('CLICK_USER'),
                                password=os.environ.get('CLICK_PWD'),
                                secure=True,
                                verify=False)

        with dwh_connection:
            dwh_table_info = dwh_connection.execute(sql)

        dwh_table_info_dict = {"rows_qty": dwh_table_info[0][0],
                               "min_date": dwh_table_info[0][1].strftime('%Y-%m-%d %H:%M:%S'),
                               "max_date": dwh_table_info[0][2].strftime('%Y-%m-%d %H:%M:%S'),
                               "railway_names": dwh_table_info[0][3],
                               "station_names": dwh_table_info[0][4]}

        return dwh_table_info_dict
    except Exception:
        logger.exception()
        return False


def sales_execution_orders_processed_info():
    """
    Получаем информацию о состоянии таблицы DWH audit._sales__execution_orders
    """

    logger.info('Получаем информацию о состоянии таблицы '
                'DWH audit._sales__execution_orders')

    try:

        sql = """SELECT
                    count() AS rows_qty,
                    min(`SVOD.Дата отправки`) AS min_date,
                    max(`SVOD.Дата отправки`) AS max_date,
                    replace(
                        arrayStringConcat(
                            arraySort(
                                groupArray(
                                    DISTINCT `Дор. Отправления`)
                            ),
                            '|'),
                        '"',
                        '') AS railway_names,
                    replace(
                        arrayStringConcat(
                            arraySort(
                                groupArray(
                                    DISTINCT `Ст. Отправления`)
                            ),
                            '|'),
                        '"',
                        '') AS station_names
                FROM
                    audit._sales__execution_orders"""

        dwh_connection = Client(host=os.environ.get('CLICK_HOST'),
                                port=os.environ.get('CLICK_PORT'),
                                database=os.environ.get('CLICK_DBNAME'),
                                user=os.environ.get('CLICK_USER'),
                                password=os.environ.get('CLICK_PWD'),
                                secure=True, verify=False)

        with dwh_connection:
            dwh_table_info = dwh_connection.execute(sql)

        dwh_table_info_dict = {"rows_qty": dwh_table_info[0][0],
                               "min_date": dwh_table_info[0][1].strftime('%Y-%m-%d %H:%M:%S'),
                               "max_date": dwh_table_info[0][2].strftime('%Y-%m-%d %H:%M:%S'),
                               "railway_names": dwh_table_info[0][3],
                               "station_names": dwh_table_info[0][4]}

        return dwh_table_info_dict

    except Exception:
        logger.exception()
        return False


def table_info_starter(table_names_list):
    """При появлении новых таблиц в списке таблиц,
    которые должен обновлять инспектор,
    создаём записи для этих таблиц в dwh_tables_info"""

    psql_connection = psycopg2.connect(host=os.environ.get('PSQL_HOST'),
                                       port=os.environ.get('PSQL_PORT'),
                                       database=os.environ.get('PSQL_DBNAME'),
                                       user=os.environ.get('PSQL_USER'),
                                       password=os.environ.get('PSQL_PWD'))
    with psql_connection:
        cur = psql_connection.cursor()
        for table_name in table_names_list:
            cur.execute(f"""SELECT
                                *
                        FROM
                            dwh_tables_info
                        WHERE
                            dwh_table_name = '{table_name}'""")

            if cur.fetchall() == []:
                dwh_table_info_dict = str({})
                cur.execute(f"""INSERT INTO
                                dwh_tables_info (dwh_table_name,
                                                table_info,
                                                wants_to_refresh,
                                                gives_information)
                            VALUES ('{table_name}', '{dwh_table_info_dict}', True, False)""")


def gives_information_status(table_name):
    """
    Устанавливает статус таблицы, запрещающий или
    разрешающий менять местами таблицы при обновлении
    audit._sales__execution_orders и audit._sales__execution_orders_tmp
    """
    psql_connection = psycopg2.connect(host=os.environ.get('PSQL_HOST'),
                                       port=os.environ.get('PSQL_PORT'),
                                       database=os.environ.get('PSQL_DBNAME'),
                                       user=os.environ.get('PSQL_USER'),
                                       password=os.environ.get('PSQL_PWD'))
    with psql_connection:
        cur = psql_connection.cursor()
        cur.execute(f"""SELECT gives_information FROM dwh_tables_info WHERE dwh_table_name = '{table_name}'""")
        return cur.fetchone()[0]


def wants_to_refresh(set_to):
    """
    Устанавливает статус таблицы, запрещающий или
    разрешающий запускать задачи на выгрузку
    """
    psql_connection = psycopg2.connect(host=os.environ.get('PSQL_HOST'),
                                       port=os.environ.get('PSQL_PORT'),
                                       database=os.environ.get('PSQL_DBNAME'),
                                       user=os.environ.get('PSQL_USER'),
                                       password=os.environ.get('PSQL_PWD'))
    with psql_connection:
        sql = f"""UPDATE
                    dwh_tables_info
                SET
                    wants_to_refresh={set_to},
                    wants_to_refresh_timestamp=now()
                WHERE
                    dwh_table_name='audit._sales__execution_orders'"""
        psql_connection.cursor().execute(sql)


table_names_list = ['audit._sales__execution_orders']
table_info_starter(table_names_list)
