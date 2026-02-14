"""
Приложение для сбора открытых данных с сайта torgi.gov.ru
"""
import os
import json
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import Json


# Загрузка переменных окружения из .env файла
load_dotenv()


def get_db_connection():
    """Установка соединения с PostgreSQL"""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    return conn


def create_tables(conn):
    """Создание таблиц в базе данных"""
    cursor = conn.cursor()
    
    # Установка схемы torgi
    cursor.execute("SET search_path TO torgi")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS datasets (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            format VARCHAR(50) NOT NULL,
            link VARCHAR(500) NOT NULL,
            data JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    cursor.close()


def fetch_datasets_list():
    """Получение списка доступных наборов данных"""
    # Ссылка на машиночитаемый список наборов данных
    url = "https://torgi.gov.ru/new/opendata/list.json"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Ошибка при получении списка наборов данных: {e}")
        return None


def fetch_dataset_data(link):
    """Получение данных из конкретного набора данных"""
    try:
        response = requests.get(link, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Ошибка при получении данных: {e}")
        return None


def save_dataset(conn, name, format_type, link, data):
    """Сохранение набора данных в базу"""
    cursor = conn.cursor()
    
    # Установка схемы torgi
    cursor.execute("SET search_path TO torgi")
    
    cursor.execute("""
        INSERT INTO datasets (name, format, link, data)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (name) DO UPDATE
        SET data = EXCLUDED.data,
            updated_at = CURRENT_TIMESTAMP
    """, (name, format_type, link, Json(data)))
    
    conn.commit()
    cursor.close()


def main():
    """Основная функция приложения"""
    print("Запуск приложения для сбора открытых данных torgi.gov.ru")
    
    # Получение списка наборов данных
    datasets_list = fetch_datasets_list()
    
    if not datasets_list:
        print("Не удалось получить список наборов данных")
        return
    
    print(f"Найдено наборов данных: {len(datasets_list)}")
    
    # Подключение к базе данных
    try:
        conn = get_db_connection()
        print("Успешное подключение к базе данных")
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return
    
    # Создание таблиц
    create_tables(conn)
    
    # Обработка каждого набора данных
    for dataset in datasets_list:
        name = dataset.get("name", "Без названия")
        format_type = dataset.get("format", "unknown")
        link = dataset.get("link", "")
        
        print(f"Обработка: {name}")
        
        # Получение данных
        data = fetch_dataset_data(link)
        
        if data:
            save_dataset(conn, name, format_type, link, data)
            print(f"  Данные сохранены: {len(data) if isinstance(data, list) else 'object'} записей")
        else:
            print(f"  Не удалось получить данные")
    
    conn.close()
    print("Завершение работы приложения")


if __name__ == "__main__":
    main()
