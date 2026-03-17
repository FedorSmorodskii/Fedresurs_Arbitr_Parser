# Fedresurs parser

Парсер (Scrapy) для получения сведений о банкротстве с `https://fedresurs.ru/` по списку ИНН из `.xlsx`.

## Обоснование выбора фреймворка

Выбран **Scrapy**, потому что он из коробки даёт асинхронную загрузку, управление конкурентностью/задержками, ретраи, middleware и pipelines. Для текущих источников достаточно HTTP-запросов и разбора JSON/HTML без полноценного браузера; при необходимости обхода сложной защиты (динамика/капча) проект можно расширить Playwright/Selenium/Camoufox.

## Что делает

Для каждого ИНН:

1) `GET /backend/persons?searchString={inn}...` получает `person_guid`  
2) `GET /backend/persons/{guid}/bankruptcy` достаёт:
   - **Номер дела** (`legalCases[].number`)
   - **последнюю дату** (максимальная `lastPublications[].datePublish` по всем делам)

Результат сохраняется в SQLite через SQLAlchemy.

## Ретраи и задержки

Scrapy уже умеет ретраить  `DOWNLOAD_DELAY`/AutoThrottle. На практике для многих задач этого **достаточно**.
Но по требованиям ТЗ добавлен **exponential backoff** на ретраях (0.5s, 1s, 2s, 4s… с небольшим джиттером, до 20s).

### Типы поиска

- **ИНН юридических лиц**: поддерживается прямой поиск по ИНН.
- **ИНН физических лиц**: поддерживается прямой поиск по ИНН.
- **Иностранные компании (аналог ИНН)**: пробовали использовать аналог ИНН, но на практике оказалось неочевидно, какие именно поля и идентификаторы нужно забирать с сайта, поэтому полноценная поддержка этого кейса пока не реализована.

## Установка

```bash
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
```

## Arbitr (kad.arbitr.ru) парсер

Парсер (Scrapy) для получения данных по делу с `https://kad.arbitr.ru/`

### Что достаёт

Для каждого номера дела:

1) `POST /Kad/SearchInstances` → достаёт ссылку на карточку дела по XPath `//a[@class="num_case"]/@href`  
2) `POST /Card/<caseId>` → пытается достать:
   - **последнюю дату**: `//span[@class="b-reg-date"]`
   - **наименование документа**: `//h2[contains(@class,"b-case-result")]/a`
3) Если на карточке нет нужных данных, делает доп. запрос:
   - `GET /Kad/InstanceDocumentsPage?...&id=<instanceId>&caseId=<caseId>&perPage=30&page=1`
   - берёт **первый “документный”** элемент (с `IsAct=true` или наличием `FileName/OriginalActFileName`)
   - дата  из `DisplayDate`
   - наименование  из `ContentTypes` (склеиваются в одну строку)

Результат сохраняется в ту же БД, в отдельную таблицу `arbitr_bankruptcy`.

### Запуск

Входные данные из Excel:

```bash
python -m src.app_arbitr --input .\\input\\cases.xlsx --db sqlite:///data/app.sqlite
```

Входные данные из командной строки:

```bash
python -m src.app_arbitr --case А32-28873/2024 --db sqlite:///data/app.sqlite
```

### Полезные параметры

- `--concurrency`, `--download-delay`: ограничения нагрузки (для arbitr задержка по умолчанию увеличена)
- `--log-level`, `--log-file`: логирование


## Запуск через Docker Compose

1) Скопируйте пример env-файла и при необходимости поправьте пути к входным `.xlsx`:

```bash
copy .env.example .env
```

2) Положите входные файлы:
- `.\input\inn.xlsx`  для Fedresurs
- `.\input\cases.xlsx`  для Kad.Arbitr

3) Поднимите БД и запустите нужный парсер.

Fedresurs (ИНН из `.xlsx`, путь передаётся через переменную/аргумент compose `INN_XLSX`):

```bash
docker compose up --build db fedresurs
```

Arbitr (номера дел из `.xlsx`, путь передаётся через `CASES_XLSX`):

```bash
docker compose up --build db arbitr
```

### Разовый запуск без `--resume` (удобно для повторного прогона)

`docker compose up ...` в этом проекте запускает сервисы с флагом `--resume`, поэтому при повторном запуске в тот же день часть/все записи могут быть пропущены как уже обработанные. Если нужно прогнать заново, используйте `docker compose run --rm` и задайте путь к файлу через переменную.

Fedresurs:

```bash
set INN_XLSX=/app/input/inn.xlsx
docker compose run --rm fedresurs ^
  python -m src.app ^
  --input %INN_XLSX% ^
  --db "postgresql+psycopg://parser:parser@db:5432/parser" ^
  --log-level INFO ^
  --log-file /app/logs/fedresurs.log
```

Arbitr:

```bash
set CASES_XLSX=/app/input/cases.xlsx
docker compose run --rm arbitr ^
  python -m src.app_arbitr ^
  --input %CASES_XLSX% ^
  --db "postgresql+psycopg://parser:parser@db:5432/parser" ^
  --log-level INFO ^
  --log-file /app/logs/arbitr.log
```

Результаты и логи:
- БД PostgreSQL  в volume `pgdata`
- Логи  `.\logs\fedresurs.log` и `.\logs\arbitr.log`


## Подготовка входного файла

Положите `.xlsx` в папку `input/`. В файле должна быть колонка с ИНН (по умолчанию ищется первая подходящая колонка, либо можно указать имя).

## Запуск

Пример:

```bash
python -m src.app --input .\\input\\inn_sample.xlsx --db sqlite:///data/app.sqlite
```

Запуск с ИНН прямо из команды:

```bash
python -m src.app --inn 231138771115 --db sqlite:///data/app.sqlite
```

Полезные параметры:

- `--concurrency`, `--download-delay`: ограничения нагрузки
- `--log-level`, `--log-file`: логирование

## Где смотреть результат

- SQLite файл: `data/app.sqlite`
- Логи: по умолчанию в stdout, либо `--log-file .\\logs\\run.log`

## Статусы

- `ok`: найдены `guid`, дело и дата
- `no_legal_cases`: найден `guid`, но `legalCases` пустой
- `not_found`: по ИНН не найдено ни одной персоны
- `error`: прочая ошибка (детали в поле `error`)


