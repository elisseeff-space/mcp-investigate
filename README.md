# mcp-investigate
mcp-investigate

## Prompt

### Introduction

⚡ This repository investigates the Multi-Cloud Platform (MCP) architecture, focusing on its design patterns, implementation details, and potential use cases. It serves as a technical exploration of how MCP enables seamless integration across multiple cloud environments.

1. Надо написать приложение на Python для выборки открытых данных c сайта https://torgi.gov.ru/new/public/opendata/reg. Используй hyperbrowser-mcp

2. Создай схему torgi для хранения данных в DB.

### --metaupdate

3. По ссылкам указанным в файле list.json лежат файлы meta.json , в которых перечислены все имеющиеся порции открытых данных с их датой. list.json лежит здесь: https://torgi.gov.ru/new/public/opendata/reg

4. Добавь в main.py функцию обновления файлов meta.json, которая должна выполняться при указании параметра --metaupdate . Эта функция должна загружать файлы meta.json с сервера, проверять их дату и обновлять локальные копии, если новая версия доступна. Файлы нужно размещать в отдельных каталогах с названием сформированным из поля link файла list.json соответствующей категории. Нужно взять подстроку от символа - и до / в нижнем регистре. Если каталогов нет, то их нужно создать.

5. После обновления meta.json, запусти парсинг данных и сохранение в базу. Для каждой категории создай отдельную таблицу в БД и заполни её данными из соответствующего файла meta.json. В создаваемых таблицах не нужно записывать в поле data json-текст. Надо создать отдельные поля таблицы для каждой записи.
Убедись, что структура таблиц соответствует структуре данных в meta.json.

### --privatizationplans

1. Создай функционал, который запускается при указании параметра --privatizationplans
2. Скачиваем данные планов приватизации и загружаем их в базу данных. По умолчанию скачиваем данные за последний день. Если нужно большее количество дней, то оно указывается цифрой после --privatizationplans. Например, --privatizationplans 7 для загрузки данных за последние 7 дней.
3. Скачиваем файл данных по ссылке указанной в поле source таблицы privatizationplans. Файл размещаем в подкаталоге ./privatizationplans/data
4. Данные из файла загружаются в таблицу privatizationplansdetail. Эта таблица связана с privatizationplans по полю privatizationplansid. Остальные поля берем из записей файла данных.  Поле href должно быть уникальным.
Вот пример одной записи файла данных:
 {
    "hostingOrg": "2200020458",
    "bidderOrgCode": "2200020458",
    "documentType": "planCancel",
    "regNum": "20250114220002045802",
    "publishDate": "2026-02-13T09:05:06.075Z",
    "href": "https://torgi.gov.ru/new/opendata/7710568760-privatizationPlans/docs/planCancel_20250114220002045802_fb4df13f-f4df-42de-810e-f783631a2006.json"
},

### --privatizationplansdocs

### --privatizationplansdocs
1. Создай функционал, который запускается при указании параметра --privatizationplansdocs
2. Необходимо обработать все записи из таблицы privatizationplansdetail и загрузить данные по ссылкам, указанным в поле href. Файлы нужно размещать в ./privatizationplans/documents
3. Для каждой записи из таблицы privatizationplansdetail, если файл ещё не был загружен, скачай его по ссылке href и сохрани в соответствующий подкаталог ./privatizationplans/documents. не меняй.
4. Данные из файла нужно записать в одну из четырех, вновь созданных таблиц в зависимости от значения поля documenttype:
- privatizationPlan;
- privatizationDecision;
- planCancel;
- planReport;
Имена таблиц сделай в нижнем регистре.
Состав полей берем из загруженных файлов данных, по тому же принципу, как грузили данные в таблицу privatizationplansdetail.
5. Если поле имеет вложенную структуру, то нужно развернуть структуру в отдельные поля таблицы. Например, если есть поле `details` с вложенными данными, создай отдельные столбцы для каждого элемента этой структуры. Имя поля формируй по правилу details_<имя вложенного поля>. Убедись, что все вложенные поля корректно отображены в таблице и соответствуют типам данных из файла.
5. При обработке файла privatizationPlan все записи поля privatizationObjects нужно записать в отдельную вновь созданную таблицу privatizationobjects.