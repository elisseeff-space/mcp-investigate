"""
Скрипт для загрузки планов приватизации за вчера
"""
import os
import json
import requests
from datetime import datetime, timedelta
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
        CREATE TABLE IF NOT EXISTS privatization_plans (
            id SERIAL PRIMARY KEY,
            date_range_start DATE,
            date_range_end DATE,
            data JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    cursor.close()


def fetch_privatization_plans_yesterday():
    """Получение планов приватизации за вчера или предыдущую доступную дату"""
    # Ссылка на метаданные планов приватизации
    meta_url = "https://torgi.gov.ru/new/opendata/7710568760-privatizationPlans/meta.json"
    
    try:
        # Получаем метаданные
        response = requests.get(meta_url, timeout=30)
        response.raise_for_status()
        meta_data = response.json()
        
        # Вычисляем вчерашнюю дату
        yesterday = datetime.now() - timedelta(days=1)
        yesterday_str = yesterday.strftime("%d.%m.%Y")
        
        print(f"Ищем данные за вчера: {yesterday_str}")
        
        # Ищем данные за вчера
        data_url = None
        date_range_start = None
        date_range_end = None
        for item in meta_data.get("data", []):
            provenance = item.get("provenance", "")
            if yesterday_str in provenance:
                data_url = item.get("source")
                # Извлекаем даты из provenance (формат: "Выгрузка размещённых планов приватизации за DD.MM.YYYY")
                # Или из URL (формат: data-YYYYMMDDT0000-YYYYMMDDT0000-...)
                import re
                # Пытаемся извлечь из URL
                url_match = re.search(r"data-(\d{8})T0000-(\d{8})T0000", data_url)
                if url_match:
                    date_range_start = url_match.group(1)
                    date_range_end = url_match.group(2)
                else:
                    date_range_start = item.get("valid")
                    date_range_end = item.get("valid")
                print(f"Найдены данные за {yesterday_str}: {data_url}")
                print(f"Диапазон дат: {date_range_start} - {date_range_end}")
                break
        
        if not data_url:
            print(f"Данные за {yesterday_str} не найдены")
            return None, None, None
        
        # Получаем данные
        response = requests.get(data_url, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        # Проверяем, что данные не пустые
        if not data or (isinstance(data, dict) and not data):
            print(f"Данные за {yesterday_str} пустые, ищем предыдущую дату")
            # Ищем предыдущую дату
            for item in meta_data.get("data", []):
                provenance = item.get("provenance", "")
                if yesterday_str not in provenance:
                    data_url = item.get("source")
                    import re
                    url_match = re.search(r"data-(\d{8})T0000-(\d{8})T0000", data_url)
                    if url_match:
                        date_range_start = url_match.group(1)
                        date_range_end = url_match.group(2)
                    else:
                        date_range_start = item.get("valid")
                        date_range_end = item.get("valid")
                    print(f"Найдены данные за предыдущую дату: {data_url}")
                    print(f"Диапазон дат: {date_range_start} - {date_range_end}")
                    break
        
        if not data_url:
            print(f"Данные за {yesterday_str} не найдены")
            return None, None, None
        
        # Получаем данные
        response = requests.get(data_url, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        # Проверяем, что данные не пустые
        if not data or (isinstance(data, dict) and not data):
            print(f"Данные за {yesterday_str} пустые")
            return None, None, None
        
        return data, date_range_start, date_range_end
        
    except requests.RequestException as e:
        print(f"Ошибка при получении данных: {e}")
        return None, None, None


def save_privatization_plans(conn, data, date_range_start, date_range_end):
    """Сохранение планов приватизации в базу"""
    cursor = conn.cursor()
    
    # Установка схемы torgi
    cursor.execute("SET search_path TO torgi")
    
    cursor.execute("""
        INSERT INTO privatization_plans (date_range_start, date_range_end, data)
        VALUES (%s, %s, %s)
        RETURNING id
    """, (date_range_start, date_range_end, Json(data)))
    
    result = cursor.fetchone()
    conn.commit()
    cursor.close()
    
    return result[0] if result else None


def main():
    """Основная функция приложения"""
    print("Запуск приложения для загрузки планов приватизации за вчера")
    
    # Получение данных
    data, date_range_start, date_range_end = fetch_privatization_plans_yesterday()
    
    if not data:
        print("Не удалось получить данные о планах приватизации")
        return
    
    print(f"Получено данных: {len(data) if isinstance(data, list) else 'object'} записей")
    print(f"Диапазон дат: {date_range_start} - {date_range_end}")
    
    # Подключение к базе данных
    try:
        conn = get_db_connection()
        print("Успешное подключение к базе данных")
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return
    
    # Создание таблиц
    create_tables(conn)
    
    # Сохранение данных
    plan_id = save_privatization_plans(conn, data, date_range_start, date_range_end)
    print(f"Данные сохранены с ID: {plan_id}")
    
    conn.close()
    print("Завершение работы приложения")


if __name__ == "__main__":
    main()
