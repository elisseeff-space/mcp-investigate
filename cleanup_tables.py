"""
Скрипт для удаления таблиц, имена которых начинаются с _
"""
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT", "5432"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

cursor = conn.cursor()
cursor.execute("SET search_path TO torgi")
cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'torgi' AND table_name LIKE '_%'")
tables = cursor.fetchall()
print('Таблицы, начинающиеся с _:', tables)

for table in tables:
    table_name = table[0]
    cursor.execute(f'DROP TABLE IF EXISTS torgi.{table_name} CASCADE')
    print(f'Удалена таблица: {table_name}')

conn.commit()
cursor.close()
conn.close()
print('Готово')