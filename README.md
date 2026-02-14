# mcp-investigate
mcp-investigate

## Prompt
⚡ This repository investigates the Multi-Cloud Platform (MCP) architecture, focusing on its design patterns, implementation details, and potential use cases. It serves as a technical exploration of how MCP enables seamless integration across multiple cloud environments.

1. Надо написать приложение на Python для выборки открытых данных c сайта https://torgi.gov.ru/new/public/opendata/reg. Используй hyperbrowser-mcp

2. Создай схему torgi для хранения данных в DB.

3. По ссылкам указанным в файле list.json лежат файлы meta.json , в которых перечислены все имеющиеся порции открытых данных с их датой. 

4. Добавь в main.py функцию обновления файлов meta.json, которая должна выполняться при указании параметра --metaupdate . Эта функция должна загружать файлы meta.json с сервера, проверять их дату и обновлять локальные копии, если новая версия доступна. Файлы нужно размещать в отдельных каталогах с названием сформированным из поля link файла list.json соответствующей категории. Нужно взять подстроку от символа - и до / в нижнем регистре. Если каталогов нет, то их нужно создать.

5. После обновления meta.json, запусти парсинг данных и сохранение в базу. Для каждой категории создай отдельную таблицу в БД и заполни её данными из соответствующего файла meta.json. В создаваемых таблицах не нужно записывать в поле data json-текст. Надо создать отдельные поля таблицы для каждой записи.
Убедись, что структура таблиц соответствует структуре данных в meta.json.
