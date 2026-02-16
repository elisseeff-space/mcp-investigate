"""
Приложение для сбора открытых данных с сайта torgi.gov.ru
"""
import os
import sys
import json
import requests
import argparse
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import Json
from psycopg2.errors import UniqueViolation


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
    # Поле source должно быть уникальным
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            source VARCHAR(500) UNIQUE,
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
        
        # Проверяем, существует ли уже запись с таким source
        cursor.execute(f"""
            SELECT id FROM {table_name}
            WHERE source = %s
        """, (source,))
        
        existing = cursor.fetchone()
        
        if existing:
            print(f"  Запись уже существует (id={existing[0]}), пропускаем")
            continue
        
        # Вставляем запись с отдельными полями
        # Используем ON CONFLICT для обработки дубликатов по source
        cursor.execute(f"""
            INSERT INTO {table_name} (source, created, provenance, valid, structure, date_range_start, date_range_end)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source) DO UPDATE
            SET created = EXCLUDED.created,
                provenance = EXCLUDED.provenance,
                valid = EXCLUDED.valid,
                structure = EXCLUDED.structure,
                date_range_start = EXCLUDED.date_range_start,
                date_range_end = EXCLUDED.date_range_end,
                created_at = CURRENT_TIMESTAMP
            RETURNING id
        """, (source, created, provenance, valid, structure, date_range_start, date_range_end))
        
        result = cursor.fetchone()
        if result:
            inserted_count += 1
            print(f"  Вставлена/обновлена запись: {provenance}")
    
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


def create_privatization_tables(conn):
    """Создание таблиц для планов приватизации"""
    cursor = conn.cursor()
    
    # Установка схемы torgi
    cursor.execute("SET search_path TO torgi")
    
    # Таблица для хранения ссылок на файлы планов приватизации
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS privatizationplans (
            id SERIAL PRIMARY KEY,
            source VARCHAR(500) UNIQUE,
            created VARCHAR(100),
            valid VARCHAR(100),
            structure VARCHAR(100),
            date_range_start VARCHAR(100),
            date_range_end VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Таблица для хранения детальных данных из файлов
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS privatizationplansdetail (
            id SERIAL PRIMARY KEY,
            privatizationplansid INTEGER REFERENCES privatizationplans(id),
            hostingOrg VARCHAR(100),
            bidderOrgCode VARCHAR(100),
            documentType VARCHAR(100),
            regNum VARCHAR(100),
            publishDate TIMESTAMP,
            href VARCHAR(500) UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    cursor.close()


def create_document_tables(conn):
    """Создание таблиц для документов планов приватизации"""
    cursor = conn.cursor()
    
    # Установка схемы torgi
    cursor.execute("SET search_path TO torgi")
    
    # Таблица privatizationplan
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS privatizationplan (
            id SERIAL PRIMARY KEY,
            privatizationplansdetail_id INTEGER REFERENCES privatizationplansdetail(id),
            schemeversion VARCHAR(100),
            id_field VARCHAR(200) UNIQUE,
            version INTEGER,
            plannumber VARCHAR(100),
            name TEXT,
            publishdate TIMESTAMP,
            timezone_code VARCHAR(100),
            timezone_name VARCHAR(200),
            signingdate VARCHAR(100),
            hostingorg_code VARCHAR(100),
            hostingorg_name TEXT,
            hostingorg_inn VARCHAR(100),
            hostingorg_kpp VARCHAR(100),
            hostingorg_ogrn VARCHAR(100),
            hostingorg_orgtype VARCHAR(100),
            planingperiod VARCHAR(100),
            startyear INTEGER,
            endyear INTEGER,
            signingdate_plan TEXT,
            documentnumber VARCHAR(100),
            ownershipforms_code VARCHAR(100),
            ownershipforms_name VARCHAR(200),
            budget_code VARCHAR(100),
            budget_name TEXT,
            budget_codeokfs VARCHAR(100),
            authority TEXT,
            budgetrevenueforecast_sumfirstyear VARCHAR(100),
            budgetrevenueforecast_sumsecondyear VARCHAR(100),
            budgetrevenueforecast_sumthirdyear VARCHAR(100),
            attachments_id VARCHAR(200),
            attachments_name TEXT,
            attachments_size INTEGER,
            attachments_hash TEXT,
            attachments_attachmenttype_code VARCHAR(200),
            attachments_attachmenttype_name TEXT,
            signeddata_id VARCHAR(200),
            signeddata_size INTEGER,
            signeddata_hash TEXT,
            signeddata_filetype VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Таблица privatizationdecision
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS privatizationdecision (
            id SERIAL PRIMARY KEY,
            privatizationplansdetail_id INTEGER REFERENCES privatizationplansdetail(id),
            schemeversion VARCHAR(100),
            id_field VARCHAR(200) UNIQUE,
            decisionnumber VARCHAR(100),
            publishdate TIMESTAMP,
            timezone_code VARCHAR(100),
            timezone_name VARCHAR(200),
            hostingorg_code VARCHAR(100),
            bidderorg_code VARCHAR(100),
            bidderorg_name TEXT,
            bidderorg_inn VARCHAR(100),
            bidderorg_kpp VARCHAR(100),
            bidderorg_ogrn VARCHAR(100),
            bidderorg_orgtype VARCHAR(100),
            bidderorg_unregistered BOOLEAN,
            privatizationreason TEXT,
            startprice VARCHAR(100),
            biddforms_code VARCHAR(100),
            biddforms_name VARCHAR(200),
            stockinfo_minusone BOOLEAN,
            privatizationobject_plannumber VARCHAR(100),
            privatizationobject_objectnumber VARCHAR(100),
            privatizationobject_name TEXT,
            privatizationobject_type VARCHAR(100),
            privatizationobject_isnotinplan BOOLEAN,
            attachments_id VARCHAR(200),
            attachments_name TEXT,
            attachments_size INTEGER,
            attachments_hash TEXT,
            attachments_attachmenttype_code VARCHAR(200),
            attachments_attachmenttype_name TEXT,
            signeddata_id VARCHAR(200),
            signeddata_size INTEGER,
            signeddata_hash TEXT,
            signeddata_filetype VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Таблица plancancel
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS plancancel (
            id SERIAL PRIMARY KEY,
            privatizationplansdetail_id INTEGER REFERENCES privatizationplansdetail(id),
            schemeversion VARCHAR(100),
            id_field VARCHAR(200) UNIQUE,
            plannumber VARCHAR(100),
            name TEXT,
            cancellationdate TIMESTAMP,
            timezone_code VARCHAR(100),
            timezone_name VARCHAR(200),
            reason TEXT,
            decisiondate TIMESTAMP,
            decisionnumber VARCHAR(100),
            attachments_id VARCHAR(200),
            attachments_name TEXT,
            attachments_size INTEGER,
            attachments_hash TEXT,
            attachments_attachmenttype_code VARCHAR(200),
            attachments_attachmenttype_name TEXT,
            signeddata_id VARCHAR(200),
            signeddata_size INTEGER,
            signeddata_hash TEXT,
            signeddata_filetype VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Таблица planreport
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS planreport (
            id SERIAL PRIMARY KEY,
            privatizationplansdetail_id INTEGER REFERENCES privatizationplansdetail(id),
            schemeversion VARCHAR(100),
            id_field VARCHAR(200) UNIQUE,
            version INTEGER,
            rootid VARCHAR(200),
            name TEXT,
            publishdate TIMESTAMP,
            signingdate VARCHAR(100),
            year INTEGER,
            timezone_code VARCHAR(100),
            timezone_name VARCHAR(200),
            hostingorg_code VARCHAR(100),
            hostingorg_name TEXT,
            hostingorg_inn VARCHAR(100),
            hostingorg_kpp VARCHAR(100),
            hostingorg_ogrn VARCHAR(100),
            hostingorg_orgtype VARCHAR(100),
            privatizationplan_number VARCHAR(100),
            privatizationplan_name TEXT,
            privatizationplan_planingperiod VARCHAR(100),
            ownershipforms_code VARCHAR(100),
            ownershipforms_name VARCHAR(200),
            budget_code VARCHAR(100),
            budget_name TEXT,
            budget_codeokfs VARCHAR(100),
            subjectrf_code VARCHAR(100),
            subjectrf_name VARCHAR(200),
            reportdata_enterprisedata_plancount INTEGER,
            reportdata_enterprisedata_excludedcount INTEGER,
            reportdata_enterprisedata_factcount INTEGER,
            reportdata_companiesdata_plancount INTEGER,
            reportdata_companiesdata_excludedcount INTEGER,
            reportdata_companiesdata_tenderedcount INTEGER,
            reportdata_companiesdata_auction_count INTEGER,
            reportdata_companiesdata_auction_startsum VARCHAR(100),
            reportdata_companiesdata_auction_salesum VARCHAR(100),
            reportdata_companiesdata_publicoffer_count INTEGER,
            reportdata_companiesdata_publicoffer_startsum VARCHAR(100),
            reportdata_companiesdata_publicoffer_salesum VARCHAR(100),
            reportdata_companiesdata_competition_count INTEGER,
            reportdata_companiesdata_competition_startsum VARCHAR(100),
            reportdata_companiesdata_competition_salesum VARCHAR(100),
            reportdata_otherpropertydata_plancount INTEGER,
            reportdata_otherpropertydata_tenderedcount INTEGER,
            reportdata_otherpropertydata_auction_count INTEGER,
            reportdata_otherpropertydata_auction_startsum VARCHAR(100),
            reportdata_otherpropertydata_auction_salesum VARCHAR(100),
            reportdata_otherpropertydata_publicoffer_count INTEGER,
            reportdata_otherpropertydata_publicoffer_startsum VARCHAR(100),
            reportdata_otherpropertydata_publicoffer_salesum VARCHAR(100),
            reportdata_otherpropertydata_competition_count INTEGER,
            reportdata_otherpropertydata_competition_startsum VARCHAR(100),
            reportdata_otherpropertydata_competition_salesum VARCHAR(100),
            reportdata_revenuesdata_planrevenues VARCHAR(100),
            reportdata_revenuesdata_planbudgetdeficitfinancingsum VARCHAR(100),
            reportdata_revenuesdata_factbudgetdeficitfinancingtotalsum VARCHAR(100),
            reportdata_revenuesdata_factbudgetdeficitfinancingthisyearsum VARCHAR(100),
            reportdata_revenuesdata_factbudgetdeficitfinancinglastyearsum VARCHAR(100),
            reportdata_revenuesdata_plannontaxrevenuesum VARCHAR(100),
            reportdata_revenuesdata_factnontaxrevenuetotalsum VARCHAR(100),
            reportdata_revenuesdata_factnontaxrevenuethisyeartotalsum VARCHAR(100),
            reportdata_revenuesdata_factnontaxrevenuelastyeartotalsum VARCHAR(100),
            attachments_id VARCHAR(200),
            attachments_name TEXT,
            attachments_size INTEGER,
            attachments_hash TEXT,
            attachments_attachmenttype_code VARCHAR(200),
            attachments_attachmenttype_name TEXT,
            signeddata_id VARCHAR(200),
            signeddata_size INTEGER,
            signeddata_hash TEXT,
            signeddata_filetype VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Таблица privatizationobjects
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS privatizationobjects (
            id SERIAL PRIMARY KEY,
            privatizationplan_id INTEGER REFERENCES privatizationplansdetail(id),
            objectnumber VARCHAR(100) UNIQUE,
            statusobject VARCHAR(100),
            name TEXT,
            type VARCHAR(100),
            timing VARCHAR(100),
            subjectrf_code VARCHAR(100),
            subjectrf_name VARCHAR(200),
            location TEXT,
            stockinfo_minusone BOOLEAN,
            purpose_code VARCHAR(100),
            purpose_name VARCHAR(200),
            kadnumber VARCHAR(100),
            attachments_id VARCHAR(200),
            attachments_name TEXT,
            attachments_size INTEGER,
            attachments_hash TEXT,
            attachments_attachmenttype_code VARCHAR(200),
            attachments_attachmenttype_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    cursor.close()


def save_privatization_data_to_db(conn, source, created, valid, structure, date, data):
    """Сохраняет данные планов приватизации в БД"""
    cursor = conn.cursor()
    
    # Установка схемы torgi
    cursor.execute("SET search_path TO torgi")
    
    # Извлекаем даты из URL
    date_range_start = None
    date_range_end = None
    
    import re
    url_match = re.search(r"data-(\d{8})T0000-(\d{8})T0000", source)
    if url_match:
        date_range_start = url_match.group(1)
        date_range_end = url_match.group(2)
    
    # Проверяем, существует ли уже запись с таким source
    cursor.execute("""
        SELECT id FROM privatizationplans
        WHERE source = %s
    """, (source,))
    
    existing = cursor.fetchone()
    
    if existing:
        print(f"  Запись в privatizationplans уже существует (id={existing[0]}), пропускаем")
        plan_id = existing[0]
    else:
        # Вставляем запись в privatizationplans
        cursor.execute("""
            INSERT INTO privatizationplans (source, created, valid, structure, date_range_start, date_range_end)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (source, created, valid, structure, date_range_start, date_range_end))
        
        result = cursor.fetchone()
        plan_id = result[0] if result else None
        print(f"  Вставлена запись в privatizationplans (id={plan_id})")
    
    if plan_id:
        # Получаем список детальных данных
        details = data.get("listObjects", []) if isinstance(data, dict) else data
        
        if details and isinstance(details, list):
            # Сохраняем детальные данные
            inserted_detail_count = 0
            for detail in details:
                hosting_org = detail.get("hostingOrg", "")
                bidder_org_code = detail.get("bidderOrgCode", "")
                document_type = detail.get("documentType", "")
                reg_num = detail.get("regNum", "")
                publish_date = detail.get("publishDate", "")
                href = detail.get("href", "")
                
                # Проверяем, существует ли уже запись с таким href
                cursor.execute("""
                    SELECT id FROM privatizationplansdetail
                    WHERE href = %s
                """, (href,))
                
                existing_detail = cursor.fetchone()
                
                if existing_detail:
                    continue
                
                # Вставляем детальную запись
                cursor.execute("""
                    INSERT INTO privatizationplansdetail (privatizationplansid, hostingOrg, bidderOrgCode, documentType, regNum, publishDate, href)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (plan_id, hosting_org, bidder_org_code, document_type, reg_num, publish_date, href))
                
                inserted_detail_count += 1
            
            print(f"  Вставлено детальных записей: {inserted_detail_count}")
    
    conn.commit()
    cursor.close()


def fetch_and_save_document_file(href, document_type):
    """
    Скачивает файл документа по ссылке и сохраняет в каталог documents.
    Возвращает путь к сохраненному файлу или None при ошибке.
    """
    import os
    
    try:
        response = requests.get(href, timeout=60)
        response.raise_for_status()
        
        # Извлекаем имя файла из href
        filename = href.split('/')[-1]
        
        # Создаем каталог для документов, если его нет
        documents_dir = os.path.join("privatizationplans", "documents")
        if not os.path.exists(documents_dir):
            os.makedirs(documents_dir)
        
        filepath = os.path.join(documents_dir, filename)
        
        # Сохраняем файл
        with open(filepath, 'wb') as f:
            f.write(response.content)
        
        return filepath
    except requests.RequestException as e:
        print(f"  Ошибка при загрузке файла {href}: {e}")
        return None
    except Exception as e:
        print(f"  Ошибка при сохранении файла {href}: {e}")
        return None


def process_document_file(filepath, document_type, conn, privatizationplansdetail_id=None):
    """
    Обрабатывает файл документа и сохраняет данные в соответствующую таблицу.
    """
    import re
    
    cursor = conn.cursor()
    cursor.execute("SET search_path TO torgi")
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"  Ошибка при чтении файла {filepath}: {e}")
        return False
    
    # Извлекаем структуру данных из exportObject.structuredObject
    structured_object = data.get("exportObject", {}).get("structuredObject", {})
    
    if not structured_object:
        print(f"  Нет структурированных данных в файле {filepath}")
        return False
    
    # Определяем тип документа и извлекаем данные
    doc_data = None
    table_name = None
    
    if document_type == "privatizationPlan":
        doc_data = structured_object.get("privatizationPlan", {})
        table_name = "privatizationplan"
    elif document_type == "privatizationDecision":
        doc_data = structured_object.get("privatizationDecision", {})
        table_name = "privatizationdecision"
    elif document_type == "planCancel":
        doc_data = structured_object.get("planCancel", {})
        table_name = "plancancel"
    elif document_type == "planReport":
        doc_data = structured_object.get("planReport", {})
        table_name = "planreport"
    
    if not doc_data:
        print(f"  Не удалось извлечь данные для типа документа {document_type}")
        return False
    
    # Добавляем privatizationplansdetail_id в данные
    if privatizationplansdetail_id is not None:
        flat_data = {"privatizationplansdetail_id": privatizationplansdetail_id}
    else:
        flat_data = {}
    
    # Обрабатываем вложенные структуры и создаем плоскую структуру полей
    
    # Обработка полей верхнего уровня
    for key, value in doc_data.items():
        if isinstance(value, dict):
            # Вложенные структуры
            if key == "commonInfo":
                # Для разных типов документов используем разные имена полей
                if document_type == "planCancel":
                    flat_data["plannumber"] = value.get("planNumber")
                    flat_data["name"] = value.get("name")
                    flat_data["cancellationdate"] = value.get("cancellationDate")
                    flat_data["timezone_code"] = value.get("timeZone", {}).get("code")
                    flat_data["timezone_name"] = value.get("timeZone", {}).get("name")
                    flat_data["signeddata_id"] = value.get("signedData", {}).get("id")
                    flat_data["signeddata_size"] = value.get("signedData", {}).get("size")
                    flat_data["signeddata_hash"] = value.get("signedData", {}).get("hash")
                    flat_data["signeddata_filetype"] = value.get("signedData", {}).get("fileType")
                elif document_type == "planReport":
                    flat_data["privatizationplan_number"] = value.get("planNumber")
                    flat_data["name"] = value.get("name")
                    flat_data["publishdate"] = value.get("publishDate")
                    flat_data["timezone_code"] = value.get("timeZone", {}).get("code")
                    flat_data["timezone_name"] = value.get("timeZone", {}).get("name")
                    flat_data["signingdate"] = value.get("signingDate")
                    flat_data["signeddata_id"] = value.get("signedData", {}).get("id")
                    flat_data["signeddata_size"] = value.get("signedData", {}).get("size")
                    flat_data["signeddata_hash"] = value.get("signedData", {}).get("hash")
                    flat_data["signeddata_filetype"] = value.get("signedData", {}).get("fileType")
                elif document_type == "privatizationDecision":
                    flat_data["schemeversion"] = doc_data.get("schemeVersion")
                    flat_data["id_field"] = doc_data.get("id")
                    flat_data["decisionnumber"] = value.get("decisionNumber")
                    flat_data["publishdate"] = value.get("publishDate")
                    flat_data["timezone_code"] = value.get("timeZone", {}).get("code")
                    flat_data["timezone_name"] = value.get("timeZone", {}).get("name")
                    flat_data["hostingorg_code"] = doc_data.get("hostingOrg", {}).get("code")
                    flat_data["bidderorg_code"] = doc_data.get("bidderOrg", {}).get("code")
                    flat_data["bidderorg_name"] = doc_data.get("bidderOrg", {}).get("name")
                    flat_data["bidderorg_inn"] = doc_data.get("bidderOrg", {}).get("INN")
                    flat_data["bidderorg_kpp"] = doc_data.get("bidderOrg", {}).get("KPP")
                    flat_data["bidderorg_ogrn"] = doc_data.get("bidderOrg", {}).get("OGRN")
                    flat_data["bidderorg_orgtype"] = doc_data.get("bidderOrg", {}).get("orgType")
                    flat_data["bidderorg_unregistered"] = doc_data.get("bidderOrg", {}).get("unregistered")
                    flat_data["privatizationreason"] = doc_data.get("privatizationReason")
                    flat_data["startprice"] = doc_data.get("startPrice")
                    flat_data["stockinfo_minusone"] = doc_data.get("stockInfo", {}).get("minusOne")
                    flat_data["privatizationobject_plannumber"] = doc_data.get("privatizationObject", {}).get("planNumber")
                    flat_data["privatizationobject_objectnumber"] = doc_data.get("privatizationObject", {}).get("objectNumber")
                    flat_data["privatizationobject_name"] = doc_data.get("privatizationObject", {}).get("name")
                    flat_data["privatizationobject_type"] = doc_data.get("privatizationObject", {}).get("type")
                    flat_data["privatizationobject_isnotinplan"] = doc_data.get("privatizationObject", {}).get("isNotInPlan")
                    flat_data["signeddata_id"] = value.get("signedData", {}).get("id")
                    flat_data["signeddata_size"] = value.get("signedData", {}).get("size")
                    flat_data["signeddata_hash"] = value.get("signedData", {}).get("hash")
                    flat_data["signeddata_filetype"] = value.get("signedData", {}).get("fileType")
                    # Обработка attachments
                    attachments = doc_data.get("attachments", [])
                    if attachments:
                        attachment = attachments[0]
                        flat_data["attachments_id"] = attachment.get("id")
                        flat_data["attachments_name"] = attachment.get("name")
                        flat_data["attachments_size"] = attachment.get("size")
                        flat_data["attachments_hash"] = attachment.get("hash")
                        attachment_type = attachment.get("attachmentType", {})
                        flat_data["attachments_attachmenttype_code"] = attachment_type.get("code")
                        flat_data["attachments_attachmenttype_name"] = attachment_type.get("name")
                    # Обработка biddForms
                    bidd_forms = doc_data.get("biddForms", [])
                    if bidd_forms:
                        bid_form = bidd_forms[0]
                        flat_data["biddforms_code"] = bid_form.get("code")
                        flat_data["biddforms_name"] = bid_form.get("name")
                else:
                    flat_data["plannumber"] = value.get("planNumber")
                    flat_data["name"] = value.get("name")
                    flat_data["publishdate"] = value.get("publishDate")
                    flat_data["timezone_code"] = value.get("timeZone", {}).get("code")
                    flat_data["timezone_name"] = value.get("timeZone", {}).get("name")
                    flat_data["signingdate"] = value.get("signingDate")
                    flat_data["signeddata_id"] = value.get("signedData", {}).get("id")
                    flat_data["signeddata_size"] = value.get("signedData", {}).get("size")
                    flat_data["signeddata_hash"] = value.get("signedData", {}).get("hash")
                    flat_data["signeddata_filetype"] = value.get("signedData", {}).get("fileType")
            elif key == "hostingOrg" and document_type != "privatizationDecision":
                flat_data["hostingorg_code"] = value.get("code")
                flat_data["hostingorg_name"] = value.get("name")
                flat_data["hostingorg_inn"] = value.get("INN")
                flat_data["hostingorg_kpp"] = value.get("KPP")
                flat_data["hostingorg_ogrn"] = value.get("OGRN")
                flat_data["hostingorg_orgtype"] = value.get("orgType")
            elif key == "planingPeriodInfo":
                flat_data["planingperiod"] = value.get("planingPeriod")
                flat_data["startyear"] = value.get("startYear")
                flat_data["endyear"] = value.get("endYear")
                flat_data["signingdate_plan"] = value.get("signingDate")
                flat_data["documentnumber"] = value.get("documentNumber")
            elif key == "ownershipForms":
                flat_data["ownershipforms_code"] = value.get("code")
                flat_data["ownershipforms_name"] = value.get("name")
            elif key == "budget":
                flat_data["budget_code"] = value.get("code")
                flat_data["budget_name"] = value.get("name")
                flat_data["budget_codeokfs"] = value.get("codeOKFS")
            elif key == "privatizationPlan":
                flat_data["privatizationplan_number"] = value.get("number")
                flat_data["privatizationplan_name"] = value.get("name")
                flat_data["privatizationplan_planingperiod"] = value.get("planingPeriod")
            elif key == "subjectRF":
                flat_data["subjectrf_code"] = value.get("code")
                flat_data["subjectrf_name"] = value.get("name")
            elif key == "reportData":
                # Обработка вложенных данных отчета
                enterprise_data = value.get("enterpriseData", {})
                flat_data["reportdata_enterprisedata_plancount"] = enterprise_data.get("planCount")
                flat_data["reportdata_enterprisedata_excludedcount"] = enterprise_data.get("excludedCount")
                flat_data["reportdata_enterprisedata_factcount"] = enterprise_data.get("factCount")
                
                companies_data = value.get("companiesData", {})
                flat_data["reportdata_companiesdata_plancount"] = companies_data.get("planCount")
                flat_data["reportdata_companiesdata_excludedcount"] = companies_data.get("excludedCount")
                flat_data["reportdata_companiesdata_tenderedcount"] = companies_data.get("tenderedCount")
                
                auction = companies_data.get("auction", {})
                flat_data["reportdata_companiesdata_auction_count"] = auction.get("count")
                flat_data["reportdata_companiesdata_auction_startsum"] = auction.get("startSum")
                flat_data["reportdata_companiesdata_auction_salesum"] = auction.get("saleSum")
                
                public_offer = companies_data.get("publicOffer", {})
                flat_data["reportdata_companiesdata_publicoffer_count"] = public_offer.get("count")
                flat_data["reportdata_companiesdata_publicoffer_startsum"] = public_offer.get("startSum")
                flat_data["reportdata_companiesdata_publicoffer_salesum"] = public_offer.get("saleSum")
                
                competition = companies_data.get("competition", {})
                flat_data["reportdata_companiesdata_competition_count"] = competition.get("count")
                flat_data["reportdata_companiesdata_competition_startsum"] = competition.get("startSum")
                flat_data["reportdata_companiesdata_competition_salesum"] = competition.get("saleSum")
                
                other_property_data = value.get("otherPropertyData", {})
                flat_data["reportdata_otherpropertydata_plancount"] = other_property_data.get("planCount")
                flat_data["reportdata_otherpropertydata_tenderedcount"] = other_property_data.get("tenderedCount")
                
                auction_op = other_property_data.get("auction", {})
                flat_data["reportdata_otherpropertydata_auction_count"] = auction_op.get("count")
                flat_data["reportdata_otherpropertydata_auction_startsum"] = auction_op.get("startSum")
                flat_data["reportdata_otherpropertydata_auction_salesum"] = auction_op.get("saleSum")
                
                public_offer_op = other_property_data.get("publicOffer", {})
                flat_data["reportdata_otherpropertydata_publicoffer_count"] = public_offer_op.get("count")
                flat_data["reportdata_otherpropertydata_publicoffer_startsum"] = public_offer_op.get("startSum")
                flat_data["reportdata_otherpropertydata_publicoffer_salesum"] = public_offer_op.get("saleSum")
                
                competition_op = other_property_data.get("competition", {})
                flat_data["reportdata_otherpropertydata_competition_count"] = competition_op.get("count")
                flat_data["reportdata_otherpropertydata_competition_startsum"] = competition_op.get("startSum")
                flat_data["reportdata_otherpropertydata_competition_salesum"] = competition_op.get("saleSum")
                
                revenues_data = value.get("revenuesData", {})
                flat_data["reportdata_revenuesdata_planrevenues"] = revenues_data.get("planRevenues")
                flat_data["reportdata_revenuesdata_planbudgetdeficitfinancingsum"] = revenues_data.get("planBudgetDeficitFinancingSum")
                flat_data["reportdata_revenuesdata_factbudgetdeficitfinancingtotalsum"] = revenues_data.get("factBudgetDeficitFinancingTotalSum")
                flat_data["reportdata_revenuesdata_factbudgetdeficitfinancingthisyearsum"] = revenues_data.get("factBudgetDeficitFinancingThisYearSum")
                flat_data["reportdata_revenuesdata_factbudgetdeficitfinancinglastyearsum"] = revenues_data.get("factBudgetDeficitFinancingLastYearSum")
                flat_data["reportdata_revenuesdata_plannontaxrevenuesum"] = revenues_data.get("planNonTaxRevenueSum")
                flat_data["reportdata_revenuesdata_factnontaxrevenuetotalsum"] = revenues_data.get("factNonTaxRevenueTotalSum")
                flat_data["reportdata_revenuesdata_factnontaxrevenuethisyeartotalsum"] = revenues_data.get("factNonTaxRevenueThisYearTotalSum")
                flat_data["reportdata_revenuesdata_factnontaxrevenuelastyeartotalsum"] = revenues_data.get("factNonTaxRevenueLastYearTotalSum")
            elif key == "privatizationObject" and document_type != "privatizationDecision":
                flat_data["privatizationobject_plannumber"] = value.get("planNumber")
                flat_data["privatizationobject_objectnumber"] = value.get("objectNumber")
                flat_data["privatizationobject_name"] = value.get("name")
                flat_data["privatizationobject_type"] = value.get("type")
                flat_data["privatizationobject_isnotinplan"] = value.get("isNotInPlan")
            elif key == "stockInfo" and document_type != "privatizationDecision":
                flat_data["stockinfo_minusone"] = value.get("minusOne")
            elif key == "purpose" and document_type != "privatizationDecision":
                flat_data["purpose_code"] = value.get("code")
                flat_data["purpose_name"] = value.get("name")
            elif key == "attachmentType" and document_type != "privatizationDecision":
                flat_data["attachments_attachmenttype_code"] = value.get("code")
                flat_data["attachments_attachmenttype_name"] = value.get("name")
        elif isinstance(value, list):
            # Обработка массивов
            if key == "attachments" and document_type != "privatizationDecision":
                # Берем первый элемент массива attachments
                if value:
                    attachment = value[0]
                    flat_data["attachments_id"] = attachment.get("id")
                    flat_data["attachments_name"] = attachment.get("name")
                    flat_data["attachments_size"] = attachment.get("size")
                    flat_data["attachments_hash"] = attachment.get("hash")
                    attachment_type = attachment.get("attachmentType", {})
                    flat_data["attachments_attachmenttype_code"] = attachment_type.get("code")
                    flat_data["attachments_attachmenttype_name"] = attachment_type.get("name")
            elif key == "biddForms" and document_type != "privatizationDecision":
                if value:
                    bid_form = value[0]
                    flat_data["biddforms_code"] = bid_form.get("code")
                    flat_data["biddforms_name"] = bid_form.get("name")
        else:
            # Простые поля верхнего уровня
            if key == "schemeVersion" and document_type != "privatizationDecision":
                flat_data["schemeversion"] = value
            elif key == "id" and document_type != "privatizationDecision":
                flat_data["id_field"] = value
            elif key == "version":
                flat_data["version"] = value
            elif key == "decisionNumber" and document_type != "privatizationDecision":
                flat_data["decisionnumber"] = value
            elif key == "publishDate" and document_type not in ("privatizationDecision", "planCancel", "planReport"):
                flat_data["publishdate"] = value
            elif key == "cancellationDate":
                flat_data["cancellationdate"] = value
            elif key == "reason":
                flat_data["reason"] = value
            elif key == "decisionDate":
                flat_data["decisiondate"] = value
            elif key == "decisionNumber":
                flat_data["decisionnumber"] = value
            elif key == "rootId":
                flat_data["rootid"] = value
            elif key == "signingDate":
                flat_data["signingdate"] = value
            elif key == "year":
                flat_data["year"] = value
            elif key == "timeZone" and document_type != "privatizationDecision":
                flat_data["timezone_code"] = value.get("code")
                flat_data["timezone_name"] = value.get("name")
            elif key == "bidderOrg" and document_type != "privatizationDecision":
                flat_data["bidderorg_code"] = value.get("code")
                flat_data["bidderorg_name"] = value.get("name")
                flat_data["bidderorg_inn"] = value.get("INN")
                flat_data["bidderorg_kpp"] = value.get("KPP")
                flat_data["bidderorg_ogrn"] = value.get("OGRN")
                flat_data["bidderorg_orgtype"] = value.get("orgType")
                flat_data["bidderorg_unregistered"] = value.get("unregistered")
            elif key == "privatizationReason" and document_type != "privatizationDecision":
                flat_data["privatizationreason"] = value
            elif key == "startPrice" and document_type != "privatizationDecision":
                flat_data["startprice"] = value
            elif key == "authority":
                flat_data["authority"] = value
            elif key == "budgetRevenueForecast":
                flat_data["budgetrevenueforecast_sumfirstyear"] = value.get("sumFirstYear")
                flat_data["budgetrevenueforecast_sumsecondyear"] = value.get("sumSecondYear")
                flat_data["budgetrevenueforecast_sumthirdyear"] = value.get("sumThirdYear")
    
    # Обработка массива privatizationObjects для типа privatizationPlan
    if document_type == "privatizationPlan" and "privatizationObjects" in doc_data:
        priv_objects = doc_data.get("privatizationObjects", [])
        privatizationplansdetail_id = flat_data.get("privatizationplansdetail_id")
        for obj in priv_objects:
            insert_privatization_object(cursor, privatizationplansdetail_id, obj)
    
    # Формируем SQL запрос для вставки
    if flat_data:
        columns = ", ".join(flat_data.keys())
        placeholders = ", ".join(["%s"] * len(flat_data))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        try:
            cursor.execute(sql, list(flat_data.values()))
            conn.commit()
            print(f"  Данные сохранены в таблицу {table_name}")
            return True
        except Exception as e:
            print(f"  Ошибка при вставке в {table_name}: {e}")
            conn.rollback()
            return False
    
    return False


def insert_privatization_object(cursor, privatizationplansdetail_id, obj):
    """Вставляет объект приватизации в таблицу privatizationobjects"""
    stock_info = obj.get("stockInfo", {})
    purpose = obj.get("purpose", {})
    attachments = obj.get("attachments", [])
    attachment_type = attachments[0].get("attachmentType", {}) if attachments else {}
    
    cursor.execute("""
        INSERT INTO privatizationobjects (
            privatizationplan_id, objectnumber, statusobject, name, type, timing,
            subjectrf_code, subjectrf_name, location, stockinfo_minusone,
            purpose_code, purpose_name, kadnumber,
            attachments_id, attachments_name, attachments_size, attachments_hash,
            attachments_attachmenttype_code, attachments_attachmenttype_name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (objectnumber) DO UPDATE SET
            privatizationplan_id = EXCLUDED.privatizationplan_id,
            statusobject = EXCLUDED.statusobject,
            name = EXCLUDED.name,
            type = EXCLUDED.type,
            timing = EXCLUDED.timing,
            subjectrf_code = EXCLUDED.subjectrf_code,
            subjectrf_name = EXCLUDED.subjectrf_name,
            location = EXCLUDED.location,
            stockinfo_minusone = EXCLUDED.stockinfo_minusone,
            purpose_code = EXCLUDED.purpose_code,
            purpose_name = EXCLUDED.purpose_name,
            kadnumber = EXCLUDED.kadnumber,
            attachments_id = EXCLUDED.attachments_id,
            attachments_name = EXCLUDED.attachments_name,
            attachments_size = EXCLUDED.attachments_size,
            attachments_hash = EXCLUDED.attachments_hash,
            attachments_attachmenttype_code = EXCLUDED.attachments_attachmenttype_code,
            attachments_attachmenttype_name = EXCLUDED.attachments_attachmenttype_name
    """, (
        privatizationplansdetail_id,
        obj.get("objectNumber"),
        obj.get("statusObject"),
        obj.get("name"),
        obj.get("type"),
        obj.get("timing"),
        obj.get("subjectRF", {}).get("code"),
        obj.get("subjectRF", {}).get("name"),
        obj.get("location"),
        stock_info.get("minusOne"),
        purpose.get("code"),
        purpose.get("name"),
        obj.get("kadNumber"),
        attachments[0].get("id") if attachments else None,
        attachments[0].get("name") if attachments else None,
        attachments[0].get("size") if attachments else None,
        attachments[0].get("hash") if attachments else None,
        attachment_type.get("code"),
        attachment_type.get("name")
    ))


def fetch_privatization_plans_data(days=1):
    """
    Загружает данные планов приватизации за указанные дни.
    По умолчанию загружает данные за последний день.
    """
    import re
    
    # Ссылка на метаданные планов приватизации
    meta_url = "https://torgi.gov.ru/new/opendata/7710568760-privatizationPlans/meta.json"
    
    try:
        # Получаем метаданные
        response = requests.get(meta_url, timeout=60)
        response.raise_for_status()
        meta_data = response.json()
        
        print(f"Получено {len(meta_data.get('data', []))} записей из meta.json")
        
        # Вычисляем даты за последние N дней
        today = datetime.now()
        dates_to_fetch = []
        for i in range(days):
            date = today - timedelta(days=i+1)
            date_str = date.strftime("%d.%m.%Y")
            dates_to_fetch.append((date, date_str))
        
        print(f"Ищем данные за последние {days} дней: {[d[1] for d in dates_to_fetch]}")
        
        # Ищем данные за указанные даты
        found_items = []
        for item in meta_data.get("data", []):
            provenance = item.get("provenance", "")
            source = item.get("source", "")
            
            # Ищем совпадение по дате в provenance
            for date_obj, date_str in dates_to_fetch:
                if date_str in provenance:
                    found_items.append({
                        "source": source,
                        "provenance": provenance,
                        "date": date_obj,
                        "created": item.get("created", ""),
                        "valid": item.get("valid", ""),
                        "structure": item.get("structure", "")
                    })
                    break
        
        if not found_items:
            print(f"Данные за последние {days} дней не найдены")
            return []
        
        print(f"Найдено {len(found_items)} записей для загрузки")
        
        # Подключение к базе данных
        try:
            conn = get_db_connection()
            print("Успешное подключение к базе данных")
        except Exception as e:
            print(f"Ошибка подключения к базе данных: {e}")
            return []
        
        # Создание таблиц
        create_privatization_tables(conn)
        
        # Создаем каталог для данных, если его нет
        data_dir = os.path.join("privatizationplans", "data")
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            print(f"Создан каталог: {data_dir}")
        
        # Загружаем данные и сохраняем в файлы и БД
        loaded_count = 0
        for item in found_items:
            source = item["source"]
            provenance = item["provenance"]
            date = item["date"]
            created = item["created"]
            valid = item["valid"]
            structure = item["structure"]
            
            print(f"\nЗагрузка: {provenance}")
            print(f"  URL: {source}")
            
            try:
                response = requests.get(source, timeout=60)
                response.raise_for_status()
                data = response.json()
                
                # Формируем имя файла на основе даты
                date_str = date.strftime("%Y%m%d")
                filename = f"privatization_{date_str}.json"
                filepath = os.path.join(data_dir, filename)
                
                # Сохраняем данные в файл
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                
                print(f"  Сохранено: {filepath} ({len(data) if isinstance(data, list) else 'object'} записей)")
                
                # Сохраняем в БД
                save_privatization_data_to_db(conn, source, created, valid, structure, date, data)
                
                loaded_count += 1
                
            except requests.RequestException as e:
                print(f"  Ошибка при загрузке данных: {e}")
            except Exception as e:
                print(f"  Ошибка при сохранении данных: {e}")
        
        conn.close()
        print(f"\nЗагружено файлов: {loaded_count}")
        return found_items
        
    except requests.RequestException as e:
        print(f"Ошибка при получении метаданных: {e}")
        return []


def fetch_privatization_plans_docs():
    """
    Загружает документы планов приватизации из таблицы privatizationplansdetail.
    Файлы сохраняются в ./privatizationplans/documents
    Данные из файлов сохраняются в соответствующие таблицы в зависимости от documentType.
    """
    import os
    
    # Подключение к базе данных
    try:
        conn = get_db_connection()
        print("Успешное подключение к базе данных")
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return
    
    # Создание таблиц для документов
    create_document_tables(conn)
    
    # Получаем все записи из таблицы privatizationplansdetail
    cursor = conn.cursor()
    cursor.execute("SET search_path TO torgi")
    cursor.execute("""
        SELECT id, privatizationplansid, hostingOrg, bidderOrgCode, documentType, regNum, publishDate, href
        FROM privatizationplansdetail
        ORDER BY id
    """)
    
    records = cursor.fetchall()
    cursor.close()
    
    print(f"Найдено {len(records)} записей для обработки")
    
    # Обрабатываем каждую запись
    processed_count = 0
    for record in records:
        record_id, plan_id, hosting_org, bidder_org_code, document_type, reg_num, publish_date, href = record
        
        # Проверяем, существует ли файл документа
        filename = href.split('/')[-1]
        documents_dir = os.path.join("privatizationplans", "documents")
        filepath = os.path.join(documents_dir, filename)
        
        if os.path.exists(filepath):
            print(f"\n[{processed_count + 1}/{len(records)}] Файл уже существует: {filename}")
        else:
            print(f"\n[{processed_count + 1}/{len(records)}] Загрузка файла: {filename}")
            
            # Скачиваем файл
            filepath = fetch_and_save_document_file(href, document_type)
            if not filepath:
                print(f"  Не удалось загрузить файл, пропускаем")
                continue
        
        # Обрабатываем файл и сохраняем данные в БД
        print(f"  Обработка файла: {filepath}")
        if process_document_file(filepath, document_type, conn, record_id):
            processed_count += 1
    
    conn.close()
    print(f"\nОбработано файлов: {processed_count}")


def main():
    """Основная функция приложения"""
    # Создаем парсер аргументов командной строки
    parser = argparse.ArgumentParser(
        description="Приложение для сбора открытых данных с сайта torgi.gov.ru",
        epilog="""
Примеры использования:
  uv run main.py --metaupdate              Обновить meta.json файлы и сохранить данные в БД
  uv run main.py --privatizationplans      Загрузить данные планов приватизации за последний день
  uv run main.py --privatizationplans 7    Загрузить данные планов приватизации за 7 дней
  uv run main.py --privatizationplansdocs  Загрузить документы планов приватизации
  uv run main.py --help                    Показать это сообщение помощи
""",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--metaupdate",
        action="store_true",
        help="Обновить meta.json файлы с сервера и сохранить данные в БД"
    )
    parser.add_argument(
        "--privatizationplans",
        nargs="?",
        const=1,
        type=int,
        metavar="DAYS",
        help="Загрузить данные планов приватизации за указанные дни (по умолчанию 1)"
    )
    parser.add_argument(
        "--privatizationplansdocs",
        action="store_true",
        help="Загрузить документы планов приватизации из таблицы privatizationplansdetail"
    )
    
    args = parser.parse_args()
    
    # Если не указано ни одного аргумента, показываем помощь
    if not args.metaupdate and args.privatizationplans is None and not args.privatizationplansdocs:
        parser.print_help()
        return
    
    # Обработка --privatizationplansdocs
    if args.privatizationplansdocs:
        print("Запуск загрузки документов планов приватизации")
        fetch_privatization_plans_docs()
        return
    
    # Обработка --privatizationplans
    if args.privatizationplans is not None:
        days = args.privatizationplans
        print(f"Запуск загрузки планов приватизации за {days} дней")
        fetch_privatization_plans_data(days)
        return
    
    # Обработка --metaupdate
    if args.metaupdate:
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
