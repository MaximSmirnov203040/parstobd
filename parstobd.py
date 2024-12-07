import requests
from datetime import datetime
import pytz
import mysql.connector
from dotenv import load_dotenv
import os

# Загрузка переменных из файла .env
load_dotenv()

# Параметры для базы данных
DB1_CONFIG = {
    "user": os.getenv("DB1_USER"),
    "password": os.getenv("DB1_PASSWORD"),
    "host": os.getenv("DB1_HOST"),
    "database": os.getenv("DB1_DATABASE")
}

DB2_CONFIG = {
    "user": os.getenv("DB2_USER"),
    "password": os.getenv("DB2_PASSWORD"),
    "host": os.getenv("DB2_HOST"),
    "database": os.getenv("DB2_DATABASE")
}

# URL для API
url = "https://quote.ru/api/v1/news/for-main-page"

# Параметры для запроса
params = {
    "latestNewsTime": int(datetime.now().timestamp() * 1000),  # Используем текущее время в миллисекундах
    "limit": 20  # Лимит на количество новостей
}


# Функция для записи данных в базу данных
def save_to_database(config, articles):
    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()

        insert_query = """
        INSERT INTO news_articles (id, date, time, title)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE title=VALUES(title);
        """

        print(f"Attempting to save {len(articles)} articles to {config['database']}...")

        cursor.executemany(insert_query, articles)
        connection.commit()

        print(f"Successfully saved {len(articles)} articles to {config['database']}.")

    except mysql.connector.Error as e:
        print(f"Error saving to database {config['database']}: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


# Функция для получения и записи данных
def get_all_news(url, params):
    all_article_ids = set()  # Для отслеживания уникальных ID
    moscow_tz = pytz.timezone("Europe/Moscow")

    while True:
        try:
            print(f"Requesting data with params: {params}")
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            print(f"Response data: {data}")

            if not data.get("data"):
                print("No more data available. Stopping...")
                break

            articles_to_save = []  # Очистка списка для текущей партии

            timestamps = []
            for item in data['data']:
                timestamp = item['publishDateTimestamp'] / 1000
                article_time = datetime.fromtimestamp(timestamp, pytz.utc).astimezone(moscow_tz)

                article_date = article_time.date()
                article_time_str = article_time.time()
                article_id = item['id']
                article_title = item.get('title', 'No title').replace('\xa0', ' ')

                if article_id in all_article_ids:
                    continue  # Пропускаем дубликаты

                # Добавляем в список для записи
                articles_to_save.append((article_id, article_date, article_time_str, article_title))
                all_article_ids.add(article_id)

                timestamps.append(item['publishDateTimestamp'])

            # Записываем текущую партию в базу данных
            if articles_to_save:
                save_to_database(DB1_CONFIG, articles_to_save)
                save_to_database(DB2_CONFIG, articles_to_save)

            # Если найдены новые статьи, обновляем latestNewsTime
            if timestamps:
                params['latestNewsTime'] = min(timestamps)
            else:
                params['latestNewsTime'] -= 1000000

        except requests.exceptions.RequestException as e:
            print(f"Error with the request: {e}")
            break
        except Exception as e:
            print(f"An error occurred: {e}")
            break


# Запускаем парсер
get_all_news(url, params)
