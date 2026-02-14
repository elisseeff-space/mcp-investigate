"""
Приложение для сбора открытых данных с сайта torgi.gov.ru
"""
import os
import sys
import json
import requests
import argparse
from datetime import datetime
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP
        )
    """)
    
    conn.commit()
    cursor.close()


def get_directory_name_from_link(link):
    """
    Извлекает имя каталога из ссылки.
    Берет подстроку от символа '-' до '/' и переводит в нижний регистр.
    Например: https://torgi.gov.ru/new/opendata/7710568760-organization/meta.json
    -> organization
    """
    # Находим последнее вхождение '-' и '/'
    last_dash = link.rfind('-')
    last_slash = link.rfind('/')
    
    if last_dash != -1 and last_slash != -1 and last_dash < last_slash:
        directory_name = link[last_dash + 1:last_slash].lower()
        return directory_name
    return None


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
        # Сначала получаем метаданные
        response = requests.get(link, timeout=60)
        response.raise_for_status()
        meta_data = response.json()
        
        # Ищем последнюю версию данных в массиве data
        data_list = meta_data.get("data", [])
        if data_list:
            # Ищем первую непустую версию данных с конца
            for i in range(len(data_list) - 1, -1, -1):
                latest_data = data_list[i]
                data_url = latest_data.get("source")
                if data_url:
                    response = requests.get(data_url, timeout=60)
                    response.raise_for_status()
                    data = response.json()
                    # Проверяем, что данные не пустые
                    if data and not (isinstance(data, dict) and not data):
                        return data
        
        return meta_data
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


def create_category_table(conn, category_name, meta_data):
    """
    Создает таблицу для конкретной категории данных.
    Структура таблицы определяется структурой данных в meta.json.
    """
    cursor = conn.cursor()
    
    # Установка схемы torgi
    cursor.execute("SET search_path TO torgi")
    
    # Формируем имя таблицы из имени категории (только латиница и подчеркивания)
    # Извлекаем только символьную часть после последнего дефиса
    table_name = category_name.replace("-", "_").lower()
    # Если имя начинается с цифры или подчеркивания, извлекаем только символьную часть
    if table_name and (table_name[0].isdigit() or table_name[0] == '_'):
        # Находим первую букву и берем подстроку от нее
        import re
        match = re.search(r'[a-z]', table_name)
        if match:
            table_name = table_name[match.start():]
    
    # Определяем структуру таблицы на основе данных
    # Создаем отдельные поля для каждого значения из meta.data
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            source VARCHAR(500),
            created VARCHAR(100),
            provenance TEXT,
            valid VARCHAR(100),
            structure VARCHAR(100),
            date_range_start VARCHAR(100),
            date_range_end VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    cursor.close()
    
    return table_name


def parse_and_save_category_data(conn, category_name, meta_data):
    """
    Парсит данные из meta.json и сохраняет в соответствующую таблицу.
    """
    cursor = conn.cursor()
    
    # Установка схемы torgi
    cursor.execute("SET search_path TO torgi")
    
    # Формируем имя таблицы
    # Извлекаем только символьную часть после последнего дефиса
    table_name = category_name.replace("-", "_").lower()
    if table_name and (table_name[0].isdigit() or table_name[0] == '_'):
        import re
        match = re.search(r'[a-z]', table_name)
        if match:
            table_name = table_name[match.start():]
    
    # Получаем список данных из meta
    data_list = meta_data.get("data", [])
    
    if not data_list:
        print(f"  Нет данных для сохранения в категории: {category_name}")
        cursor.close()
        return 0
    
    inserted_count = 0
    
    for item in data_list:
        source = item.get("source", "")
        created = item.get("created", "")
        provenance = item.get("provenance", "")
        valid = item.get("valid", "")
        structure = item.get("structure", "")
        
        # Извлекаем даты из URL
        date_range_start = None
        date_range_end = None
        
        import re
        url_match = re.search(r"data-(\d{8})T0000-(\d{8})T0000", source)
        if url_match:
            date_range_start = url_match.group(1)
            date_range_end = url_match.group(2)
        
        # Проверяем, существует ли уже запись с такими датами
        cursor.execute(f"""
            SELECT id FROM {table_name}
            WHERE date_range_start = %s AND date_range_end = %s
        """, (date_range_start, date_range_end))
        
        existing = cursor.fetchone()
        
        if existing:
            print(f"  Запись уже существует (id={existing[0]}), пропускаем")
            continue
        
        # Вставляем запись с отдельными полями
        cursor.execute(f"""
            INSERT INTO {table_name} (source, created, provenance, valid, structure, date_range_start, date_range_end)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (source, created, provenance, valid, structure, date_range_start, date_range_end))
        
        inserted_count += 1
        print(f"  Вставлена запись: {provenance}")
    
    conn.commit()
    cursor.close()
    
    return inserted_count


def fetch_privatization_plans_yesterday():
    """Получение планов приватизации за вчера"""
    # Ссылка на метаданные планов приватизации
    meta_url = "https://torgi.gov.ru/new/opendata/7710568760-privatizationPlans/meta.json"
    
    try:
        # Получаем метаданные
        response = requests.get(meta_url, timeout=30)
        response.raise_for_status()
        meta_data = response.json()
        
        print(f"Метаданные планов приватизации: {meta_data}")
        
        # Ищем ссылку на данные за вчера
        # В структуре meta_data должны быть ссылки на файлы с данными
        # Нужно найти ту, которая соответствует вчерашней дате
        
        return meta_data
    except requests.RequestException as e:
        print(f"Ошибка при получении метаданных: {e}")
        return None


def update_meta_files(datasets_list, force_update=False):
    """
    Обновляет файлы meta.json с сервера.
    Если force_update=True, обновляет все файлы.
    Возвращает список обновленных или существующих meta данных.
    """
    meta_list = datasets_list.get("meta", [])
    updated_meta = []
    
    print(f"Найдено наборов данных: {len(meta_list)}")
    
    for dataset in meta_list:
        if not isinstance(dataset, dict):
            continue
            
        name = dataset.get("title", "Без названия")
        link = dataset.get("link", "")
        
        print(f"\nОбработка: {name}")
        
        # Получаем имя каталога из ссылки
        directory_name = get_directory_name_from_link(link)
        if not directory_name:
            print(f"  Не удалось извлечь имя каталога из ссылки: {link}")
            continue
        
        # Создаем путь к каталогу
        directory_path = os.path.join(directory_name)
        
        # Создаем каталог, если он не существует
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            print(f"  Создан каталог: {directory_path}")
        
        # Путь к локальному файлу meta.json
        local_meta_path = os.path.join(directory_path, "meta.json")
        
        # Загружаем meta.json с сервера
        try:
            response = requests.get(link, timeout=60)
            response.raise_for_status()
            server_meta = response.json()
        except requests.RequestException as e:
            print(f"  Ошибка при загрузке meta.json с сервера: {e}")
            # Если файл существует локально, используем его
            if os.path.exists(local_meta_path):
                print(f"  Используем локальный файл: {local_meta_path}")
                try:
                    with open(local_meta_path, 'r', encoding='utf-8') as f:
                        local_meta = json.load(f)
                    updated_meta.append(local_meta)
                except Exception as e:
                    print(f"  Ошибка при чтении локального файла: {e}")
                continue
            else:
                print(f"  Нет локального файла, пропускаем")
                continue
        
        # Проверяем дату модификации
        server_modified = server_meta.get("modified", "")
        
        if os.path.exists(local_meta_path):
            try:
                with open(local_meta_path, 'r', encoding='utf-8') as f:
                    local_meta = json.load(f)
                local_modified = local_meta.get("modified", "")
                
                if server_modified == local_modified and not force_update:
                    print(f"  Файл meta.json не изменился (modified: {local_modified}), пропускаем")
                    updated_meta.append(local_meta)
                    continue
                else:
                    print(f"  Обнаружены изменения (server: {server_modified}, local: {local_modified}), обновляем")
            except Exception as e:
                print(f"  Ошибка при чтении локального файла: {e}, обновляем")
        
        # Сохраняем обновленный meta.json
        try:
            with open(local_meta_path, 'w', encoding='utf-8') as f:
                json.dump(server_meta, f, ensure_ascii=False, indent=4)
            print(f"  Файл обновлен: {local_meta_path} (modified: {server_modified})")
            updated_meta.append(server_meta)
        except Exception as e:
            print(f"  Ошибка при сохранении файла: {e}")
    
    return updated_meta


def main():
    """Основная функция приложения"""
    # Создаем парсер аргументов командной строки
    parser = argparse.ArgumentParser(
        description="Приложение для сбора открытых данных с сайта torgi.gov.ru",
        epilog="""
Примеры использования:
  uv run main.py --metaupdate    Обновить meta.json файлы и сохранить данные в БД
  uv run main.py --help          Показать это сообщение помощи
        """
    )
    parser.add_argument(
        "--metaupdate",
        action="store_true",
        help="Обновить meta.json файлы с сервера и сохранить данные в БД"
    )
    
    args = parser.parse_args()
    
    # Если не указано ни одного аргумента, показываем помощь
    if not args.metaupdate:
        parser.print_help()
        return
    
    print("Запуск приложения для сбора открытых данных torgi.gov.ru")
    
    # Получение списка наборов данных
    datasets_list = fetch_datasets_list()
    
    if not datasets_list:
        print("Не удалось получить список наборов данных")
        return
    
    print(f"Тип данных: {type(datasets_list)}")
    
    # Подключение к базе данных
    try:
        conn = get_db_connection()
        print("Успешное подключение к базе данных")
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return
    
    # Создание таблиц
    create_tables(conn)
    
    # Обновление meta.json файлов
    print("\n=== Обновление meta.json файлов ===")
    meta_list = datasets_list.get("meta", [])
    update_meta_files(datasets_list, force_update=True)
    
    # Парсинг данных из обновленных meta.json и сохранение в БД
    print("\n=== Парсинг данных и сохранение в БД ===")
    
    for dataset in meta_list:
        if not isinstance(dataset, dict):
            continue
            
        name = dataset.get("title", "Без названия")
        link = dataset.get("link", "")
        identifier = dataset.get("identifier", "")
        
        print(f"\nОбработка категории: {name}")
        
        # Получаем имя каталога
        directory_name = get_directory_name_from_link(link)
        if not directory_name:
            print(f"  Не удалось извлечь имя каталога из ссылки: {link}")
            continue
        
        # Путь к локальному файлу meta.json
        local_meta_path = os.path.join(directory_name, "meta.json")
        
        if not os.path.exists(local_meta_path):
            print(f"  Файл meta.json не найден: {local_meta_path}")
            continue
        
        # Загружаем meta.json
        try:
            with open(local_meta_path, 'r', encoding='utf-8') as f:
                meta_data = json.load(f)
        except Exception as e:
            print(f"  Ошибка при чтении meta.json: {e}")
            continue
        
        # Создаем таблицу для категории
        table_name = create_category_table(conn, identifier, meta_data)
        print(f"  Таблица: {table_name}")
        
        # Парсим и сохраняем данные
        inserted_count = parse_and_save_category_data(conn, identifier, meta_data)
        print(f"  Всего вставлено записей: {inserted_count}")
    
    conn.close()
    print("\nЗавершение работы приложения")


if __name__ == "__main__":
    main()
