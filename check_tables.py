"""
Скрипт для проверки таблиц в базе данных
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

# Проверим таблицы
cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'torgi' ORDER BY table_name")
tables = cursor.fetchall()
print('Таблицы в схеме torgi:')
for table in tables:
    print(f'  {table[0]}')

# Проверим структуру таблиц
for table in ['privatizationplans', 'privatizationplansdetail']:
    cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'torgi' AND table_name = '{table}' ORDER BY ordinal_position")
    columns = cursor.fetchall()
    print(f'\nСтруктура {table}:')
    for col in columns:
        print(f'  {col[0]}: {col[1]}')

cursor.close()
conn.close()