"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv

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


def write_data(connection):
    try:
        cursor = connection.cursor()
        with open('north_data/customers_data.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                cursor.execute(
                    "INSERT INTO customers (customer_id, company_name, contact_name) VALUES (%s, %s, %s)",
                    (row[0], row[1], row[2])
                )
        with open('north_data/employees_data.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                cursor.execute(
                    "INSERT INTO employees (employee_id,first_name,last_name,title,birth_date,notes) VALUES (%s, %s, %s, %s, %s, %s)",
                    (row[0], row[1], row[2], row[3], row[4], row[5])
                )
        with open('north_data/orders_data.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                cursor.execute(
                    "INSERT INTO orders (order_id,customer_id,employee_id,order_date,ship_city) VALUES (%s, %s, %s, %s, %s)",
                    (row[0], row[1], row[2], row[3], row[4])
                )
        connection.commit()
    except psycopg2.Error as e:
        print(e)


def main():
    connection = create_connection()
    if connection:
        execute_sql_file(connection, "create_tables.sql")
        write_data(connection)
        connection.close()


if __name__ == "__main__":
    main()