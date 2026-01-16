import json
from kafka import KafkaConsumer
import psycopg2
from psycopg2 import sql, OperationalError, IntegrityError, DataError
from psycopg2.extras import execute_values

KAFKA_BOOTSTRAP_SERVERS = ['10.49.192.209:9092']
KAFKA_TOPIC = 'tables'

DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'lab'
DB_USER = 'postgres'
DB_PASSWORD = '1800'

def infer_column_types(rows):
    if not rows:
        return {}
    first_row = rows[0]
    types = {}
    for col, val in first_row.items():
        if val is None:
            types[col] = 'TEXT'
        elif isinstance(val, bool):
            types[col] = 'BOOLEAN'
        elif isinstance(val, int):
            types[col] = 'INTEGER'
        elif isinstance(val, float):
            types[col] = 'DOUBLE PRECISION'
        elif isinstance(val, str):
            if len(val) >= 10 and '-' in val or ':' in val:
                if ' ' in val or 'T' in val:
                    types[col] = 'TIMESTAMP'
                else:
                    types[col] = 'DATE'
            else:
                types[col] = 'TEXT'
        else:
            types[col] = 'TEXT'
    return types

def create_table(conn, table_name, columns, col_types):
    columns_def = ', '.join(f'"{col}" {col_types.get(col, "TEXT")}' for col in columns)
    create_query = sql.SQL(
        "CREATE TABLE IF NOT EXISTS {} ({})"
    ).format(
        sql.Identifier(table_name),
        sql.SQL(columns_def)
    )

    try:
        with conn.cursor() as cur:
            cur.execute(create_query)
            conn.commit()
            print(f"Таблица '{table_name}' создана или уже существует.")
    except Exception as e:
        print(f"Ошибка создания таблицы '{table_name}': {e}")
        conn.rollback()
        return False
    return True

def insert_data(conn, table_name, columns, rows):
    insert_query = sql.SQL(
        "INSERT INTO {} ({}) VALUES %s"
    ).format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(sql.Identifier(c) for c in columns)
    )
    data_tuples = []
    for row in rows:
        try:
            data_tuples.append(tuple(row.get(col) for col in columns))
        except Exception as e:
            continue
    if not data_tuples:
        return

    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_query, data_tuples)
            conn.commit()
    except (IntegrityError, DataError) as e:
        print(f"Ошибка вставки в '{table_name}': {e}")
        conn.rollback()
    except Exception as e:
        conn.rollback()


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='etl-consumer-group-universal',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD
        )
        print("Подключение к PostgreSQL успешно.")
    except OperationalError as e:
        return

    for message in consumer:
        try:
            data = message.value
            print(f"Получено сообщение: {json.dumps(data, ensure_ascii=False, indent=2)}")

            if not isinstance(data, dict):
                continue

            processed = 0

            for table_name, rows in data.items():
                if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
                    continue
                columns = list(rows[0].keys())
                col_types = infer_column_types(rows)

                if not create_table(conn, table_name, columns, col_types):
                    continue
                insert_data(conn, table_name, columns, rows)
                processed += 1

            if processed == 0:
                print("В сообщении не найдено ни одной подходящей таблицы")

        except json.JSONDecodeError as e:
            print(f"Ошибка парсинга JSON: {e}")
        except Exception as e:
            print(f"Общая ошибка обработки сообщения: {e}")
    conn.close()


if __name__ == "__main__":
    main()