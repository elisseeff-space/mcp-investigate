"""
Скрипт для добавления колонки provenance в таблицу privatizationplans
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

# Добавляем колонку provenance
cursor.execute("ALTER TABLE privatizationplans ADD COLUMN provenance character varying")

conn.commit()
cursor.close()
conn.close()
print("Колонка provenance добавлена в таблицу privatizationplans")