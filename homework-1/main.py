"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv

conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='12345')


def add_customers():
    """
    Подключение и запись данных в БД таблица customers.
    Данные берутся из "north_data/customers_data.csv"
    """
    with open("north_data/customers_data.csv", "r", encoding="UTF8", newline='') as cust:
        reader = csv.DictReader(cust)
        for row in reader:
            customer_id = row["customer_id"]
            company_name = row["company_name"]
            contact_name = row["contact_name"]
            with conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO customers VALUES (%s, %s, %s)", (customer_id, company_name, contact_name))
                    cur.execute("SELECT * FROM customers")
                    cur.fetchall()

        conn.close()


def add_employees():
    """
    Подключение и запись данных в БД таблица employees.
    Данные берутся из "north_data/employees_data.csv"
    """
    with open("north_data/employees_data.csv", "r", encoding="UTF8", newline='') as emp:
        reader = csv.DictReader(emp)
        for row in reader:
            employee_id: int = row["employee_id"]
            first_name = row["first_name"]
            last_name = row["last_name"]
            title = row["title"]
            birth_date = row["birth_date"]
            notes = row["notes"]

            with conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                                (employee_id, first_name, last_name,
                                 title, birth_date, notes))
                    cur.execute("SELECT * FROM employees")
                    cur.fetchall()

        conn.close()


def add_orders():
    """
    Подключение и запись данных в БД таблица orders.
    Данные берутся из "north_data/orders_data.csv"
    """
    with open("north_data/orders_data.csv", "r", encoding="UTF8", newline='') as orders:
        reader = csv.DictReader(orders)
        for row in reader:
            order_id: int = row["order_id"]
            customer_id: int = row["customer_id"]
            employee_id = row["employee_id"]
            order_date = row["order_date"]
            ship_city = row["ship_city"]
            with conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", (order_id, customer_id,
                                                                                   employee_id, order_date,
                                                                                   ship_city))
                    cur.execute("SELECT * FROM orders")
                    cur.fetchall()

        conn.close()


if __name__ == "__main__":
    add_customers()
    add_employees()
    add_orders()
