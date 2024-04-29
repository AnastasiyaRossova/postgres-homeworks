"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2


def create_connection():
    try:
        connection = psycopg2.connect(
            dbname="north",
            user="postgres",
            password="1234",
            host="localhost",
            port="5432"
        )
        return connection
    except psycopg2.Error as e:
        print("Ошибка подключения:", e)
        return None


def execute_sql_file(connection, file_name):
    try:
        cursor = connection.cursor()
        with open(file_name, 'r') as file:
            cursor.execute(file.read())
        connection.commit()
        cursor.close()
    except psycopg2.Error as e:
        print(e)


def main():
    connection = create_connection()
    if connection:
        execute_sql_file(connection, "create_tables.sql")
        connection.close()


if __name__ == "__main__":
    main()