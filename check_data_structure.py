"""
Скрипт для проверки структуры данных планов приватизации
"""
import json
import requests

# Загрузим meta.json
meta_url = "https://torgi.gov.ru/new/opendata/7710568760-privatizationPlans/meta.json"
response = requests.get(meta_url, timeout=60)
meta_data = response.json()

# Получим первую ссылку на данные
if meta_data.get("data"):
    first_item = meta_data["data"][0]
    data_url = first_item.get("source")
    print(f"URL: {data_url}")
    
    # Загрузим данные
    response = requests.get(data_url, timeout=60)
    data = response.json()
    
    # Покажем полную структуру данных
    print(f"Полная структура данных:")
    print(json.dumps(data, indent=2, ensure_ascii=False)[:2000])        print(f"Первый элемент: {json.dumps(data[0], indent=2, ensure_ascii=False)[:1000]}")
