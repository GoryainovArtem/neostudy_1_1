import csv
import logging
import time
from datetime import datetime
from logging import getLogger

import psycopg2

from config import postgres_creds

logger = getLogger(__name__)
logger.setLevel(logging.INFO)

SCCS_LAYER = "scss"
TABLE_NAMES = ["ft_balance_f", "md_account_d",
               "md_currency_d", "md_exchange_rate_d",
               "md_ledger_account_s"]


def get_pg_connection():
    """
    Подключиться к базе данных bank в СУБД PostgreSQL.
    :return:
            conn - объект подключения;
            cur - курсор для выполнения SQL запросов
    """

    logger.info("Выполнить подключение к PostgreSQL.")
    conn = psycopg2.connect(
        host=postgres_creds['host'],
        user=postgres_creds['user'],
        password=postgres_creds['password'],
        port=postgres_creds['port'],
        database=postgres_creds['database']
    )
    conn.autocommit = True
    cur = conn.cursor()
    logger.info("Создан объект cursor")
    return conn, cur


def read_csv(filename: str):
    """
    Считать данные из CSV файла и вернуть сформированный список
    построчных значений.
    :param filename: имя файла, из которого поступает информация.
    :return:
            list[str] - список строк с информацией из CSV файла.
    """

    with open(f"./data/{filename}", 'r') as file:
        reader = csv.reader(file)
        next(reader)
        file_data = list(reader)
        logger.info(f"Из файла ./data/{filename}.csv было считано {len(file_data)} строк")
        return [row[0].split(";")[1:] for row in file_data]


def load_scss(target_table_name):
    """
    Загрузить данные в scss слой хранилища.
    :param target_table_name: название целевой таблице в слое scss.
    :return:
    """

    scss_table = f"{SCCS_LAYER}.{target_table_name}"
    data = read_csv(f"{target_table_name}.csv")
    conn, cur = get_pg_connection()
    logger.info(f"Создать таблицу {scss_table}")
    cur.execute(f"CREATE TABLE IF NOT EXISTS {scss_table} AS SELECT * FROM ds.{target_table_name};")
    cur.execute(f"TRUNCATE TABLE {scss_table}")
    values_template = ("%s," * len(data[0])).rstrip(',')
    logger.info(f"Выполнить вставку данных в таблицу {scss_table} из CSV файла.")
    cur.executemany(f"INSERT INTO {scss_table} VALUES({values_template})", data)
    cur.execute(f"SELECT COUNT(*) FROM {scss_table}")
    inserted_amount = cur.fetchone()[0]
    logger.info(f"В таблицу {scss_table} было записано {inserted_amount}")
    conn.close()


def load_ds(table_name: str):
    """
    Загрузить данные из таблицы в слое scss в слой ds.
    :param table_name:  имя целевой таблицы в слое ds для загрузки
    данных.
    :return:
    """

    conn, cur = get_pg_connection()
    logger.info(f"Выполнить перенос данных в таблицу ds.{table_name} из scss.{table_name}")
    cur.execute(f"SELECT upd_insert('scss.{table_name}', 'ds.{table_name}')")
    cur.execute(f"SELECT COUNT(*) FROM ds.{table_name}")
    logger.info(f"В таблицу ds.{table_name} было записано {cur.fetchone()[0]} записей.")
    conn.close()


def load_ds_dt_posting_f():
    """Загрузка данных в таблицу ds.dt_posting_f."""

    conn, cur = get_pg_connection()
    logger.info(f"Выполнить перенос данных в таблицу ds.ft_posting_f из scss.ft_posting_f")
    cur.execute(f"""
                    INSERT INTO ds.ft_posting_f 
                    SELECT DISTINCT oper_date, credit_account_rk, debet_account_rk, credit_amount, debet_amount FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK ORDER BY rn DESC) as part_rn
                    FROM (SELECT *,
                        ROW_NUMBER() OVER () as rn
                        from scss.FT_POSTING_F 
                         ) t
                    ) WHERE part_rn = 1
                    ON CONFLICT (OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK) DO UPDATE SET
                    credit_amount = EXCLUDED.credit_amount,
                    debet_amount = EXCLUDED.debet_amount;
                """
                )
    cur.execute(f"SELECT COUNT(*) FROM ds.ft_posting_f")
    logger.info(f"В таблицу ds.ft_posting_f было записано {cur.fetchone()[0]} записей.")
    conn.close()


def main():
    """
    Загрузить данные во все таблицы.
    :return:
    """
    
    print("Начало выполнения загрузки")
    now = time.perf_counter()
    conn, cur = get_pg_connection()
    cur.callproc("log_etl_execution", [])
    run_id = cur.fetchone()[0]
    time.sleep(5)
    load_scss("FT_POSTING_F")
    load_ds_dt_posting_f()
    for table in TABLE_NAMES:
        load_scss(table)
        load_ds(table)
    cur.execute(f"UPDATE logs.etl_execution SET end_dttm = '{datetime.now(tz=None)}' "
                f"WHERE run_id = {run_id}")
    conn.close()
    print("Окончание загрузки", time.perf_counter() - now)


if __name__ == "__main__":
    main()
