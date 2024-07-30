import sys
import logging
import os
from datetime import datetime
from dwh import (sales_execution_orders_source_info,
                 sales_execution_orders_processed_info,
                 sales_execution_orders_tmp_info,
                 update_dwh_table_info,
                 gives_information_status,
                 wants_to_refresh)
from sql import sql_sales__execution_orders
from dotenv import load_dotenv
from clickhouse_driver import Client
from logging.handlers import RotatingFileHandler
from clickhouse_driver.errors import ServerException
from time import sleep


basedir = os.path.abspath(os.path.dirname(__file__))

if os.path.exists('.env'):
    load_dotenv(os.path.join(basedir, '.env'))
else:
    load_dotenv(os.path.join(basedir, '.envexample'))


if not os.path.exists('logs'):
    os.mkdir('logs')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")

file_handler = RotatingFileHandler('logs/tksva_p.log',
                                   maxBytes=20480,
                                   backupCount=10)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def audit_sales__execution_orders_refresher(sales_execution_orders_source_info_dict):
    """
    Функция обновляет таблицы DWH
    """

    dwh_table_info_railway_names_list = sales_execution_orders_source_info_dict['railway_names'].split("|")

    dwh_connection = Client(host=os.environ.get('CLICK_HOST'),
                            port=os.environ.get('CLICK_PORT'),
                            database=os.environ.get('CLICK_DBNAME'),
                            user=os.environ.get('CLICK_USER'),
                            password=os.environ.get('CLICK_PWD'),
                            secure=True, verify=False)

    with dwh_connection as conn:
        logger.info('Удаляем audit._sales__execution_orders_tmp')
        conn.execute("DROP TABLE IF EXISTS audit._sales__execution_orders_tmp")

    dwh_connection = Client(host=os.environ.get('CLICK_HOST'),
                            port=os.environ.get('CLICK_PORT'),
                            database=os.environ.get('CLICK_DBNAME'),
                            user=os.environ.get('CLICK_USER'),
                            password=os.environ.get('CLICK_PWD'),
                            secure=True, verify=False)


    with dwh_connection as conn:
        logger.info('Создаём по первой дороге audit._sales__execution_orders_tmp')
        conn.execute(sql_sales__execution_orders.sql_1(dwh_table_info_railway_names_list[0]))

        n = len(dwh_table_info_railway_names_list[1:])
        i = 0
        for dwh_table_info_railway_name in dwh_table_info_railway_names_list[1:]:
            i += 1
            with dwh_connection as conn:
                try:
                    logger.info(f'Шаг {i} из {n}. Добавляем в audit._sales__execution_orders_tmp '
                                f'данные по дороге {dwh_table_info_railway_name}')
                    conn.execute(sql_sales__execution_orders.sql_2(dwh_table_info_railway_name))
                    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                    # if i == 2: break
                    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                except Exception:
                    logger.exception()
                    return False
                
        if sales_execution_orders_source_info() != sales_execution_orders_tmp_info():
            return False
                


        dwh_connection = Client(host=os.environ.get('CLICK_HOST'),
                                port=os.environ.get('CLICK_PORT'),
                                database=os.environ.get('CLICK_DBNAME'),
                                user=os.environ.get('CLICK_USER'),
                                password=os.environ.get('CLICK_PWD'),
                                secure=True, verify=False)

        with dwh_connection as conn:
            try:
                logger.info('Нумеруем строки audit._sales__execution_orders_tmp')
                conn.execute("""CREATE OR REPLACE TABLE
                                    audit._sales__execution_orders_tmp
                                ENGINE = MergeTree()
                                ORDER BY `rownumber`
                                AS (SELECT
                                        ROW_NUMBER() OVER() AS rownumber,
                                        *
                                    FROM
                                        audit._sales__execution_orders_tmp)""")
            except Exception:
                logger.exception()
                return False

        dwh_connection = Client(host=os.environ.get('CLICK_HOST'),
                                port=os.environ.get('CLICK_PORT'),
                                database=os.environ.get('CLICK_DBNAME'),
                                user=os.environ.get('CLICK_USER'),
                                password=os.environ.get('CLICK_PWD'),
                                secure=True, verify=False)

        with dwh_connection as conn:
            try:
                logger.info('Переименовываем audit._sales__execution_orders_tmp данные в audit._sales__execution_orders')
                conn.execute("RENAME TABLE audit._sales__execution_orders_tmp TO audit._sales__execution_orders;")
            except ServerException:
                logger.warning('Переименовать не получилось, потому что '
                               'audit._sales__execution_orders уже существует')
            except Exception:
                logger.exception()

        # Блокируем возможность запускать задачи на выгрузку данных
        wants_to_refresh(True)

        i = 0
        while True:
            try:
                logger.info('Проверяем, что audit._sales__execution_orders '
                            'не отдаёт информацию')

                check_start = datetime.now()

                while True:
                    if not gives_information_status('audit._sales__execution_orders'):
                        break

                    if (datetime.now()-check_start).total_seconds() > 5:
                        logger.info('Ждём пока audit._sales__execution_orders '
                                    'отдаст информацию')
                        check_start = datetime.now()

                logger.info('audit._sales__execution_orders '
                            'не отдаёт информацию')
                break
            except Exception:
                i += 1
                logger.error(f'audit._sales__execution_orders не отдаёт информацию - '
                             f'проверка не удалась (попытка {i})')
                if i > 10:
                    return False
                sleep(60)

        dwh_connection = Client(host=os.environ.get('CLICK_HOST'),
                                port=os.environ.get('CLICK_PORT'),
                                database=os.environ.get('CLICK_DBNAME'),
                                user=os.environ.get('CLICK_USER'),
                                password=os.environ.get('CLICK_PWD'),
                                secure=True, verify=False)

        with dwh_connection as conn:
            try:
                logger.info('Меняем местами audit._sales__execution_orders и '
                            'audit._sales__execution_orders_tmp')
                conn.execute("""EXCHANGE TABLES
                                audit._sales__execution_orders_tmp AND
                                audit._sales__execution_orders;""")
            except Exception:
                logger.exception()
                return False

        # Разблокируем возможность запускать задачи на выгрузку данных
        wants_to_refresh(False)

        with dwh_connection as conn:
            try:
                logger.info('Удаляем audit._sales__execution_orders_tmp')
                conn.execute("DROP TABLE audit._sales__execution_orders_tmp;")
            except Exception:
                logger.exception()

        return True


if __name__ == "__main__":

    logger.info('*'*100)
    logger.info('проверяем идентичность данных в таблицах')

    sales_execution_orders_source_info_dict = sales_execution_orders_source_info()
    if not sales_execution_orders_source_info_dict:
        sys.exit()

    sales_execution_orders_processed_info_dict = sales_execution_orders_processed_info()
    if not sales_execution_orders_processed_info_dict:
        sys.exit()

    try:
        for k, v in sales_execution_orders_source_info_dict.items():
            logger.info(f'{k}, {sales_execution_orders_processed_info_dict[k]==v}')
            # test_action = 7/0

        # if sales_execution_orders_source_info_dict == sales_execution_orders_processed_info_dict:
        if (sales_execution_orders_source_info_dict != sales_execution_orders_processed_info_dict):
            logger.info('Таблица audit._sales__execution_orders '
                        'нуждается в обновлении.')
            if audit_sales__execution_orders_refresher(sales_execution_orders_source_info_dict):
                update_dwh_table_info()
                logger.info('Готово!')
            else:
                logger.error('Не удалось обновить '
                             'audit._sales__execution_orders.')
        else:
            logger.info('Таблица audit._sales__execution_orders '
                        'содержит актуальные данные.')
    except Exception:
        logger.exception()
