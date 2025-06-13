import psycopg2
import csv
import re
from dotenv import load_dotenv
import os

load_dotenv()

CSV_FILE_PATH = r'C:\Users\user1\Desktop\openmeteo\_supabase_lobnya\russian_cities.csv'
BATCH_SIZE = 1000

def clean_population(population):
    # Удаляем все нечисловые символы из строки
    return re.sub(r'[^\d]', '', population)

def process_batch(conn, batch):
    with conn.cursor() as cursor:
        # Очистка буфера
        cursor.execute("TRUNCATE TABLE lbn.city_buffer")
        conn.commit()

        # Загрузка в буферную таблицу
        for row in batch:
            # Очистка данных о населении
            row[3] = clean_population(row[3])
            args_str = cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", row).decode('utf-8')
            cursor.execute(f"INSERT INTO lbn.city_buffer VALUES {args_str}")
        conn.commit()

        # Перенос в основную таблицу с обновлением
        cursor.execute("""
            INSERT INTO lbn.city (city_name, region, federal_district, population, foundation_year, status, old_name, latitude, longitude)
            SELECT DISTINCT ON (latitude, longitude) city_name, region, federal_district, population::INT, foundation_year, status, old_name, latitude::FLOAT, longitude::FLOAT
            FROM lbn.city_buffer
            ON CONFLICT (latitude, longitude) DO UPDATE SET
                city_name = EXCLUDED.city_name,
                region = EXCLUDED.region,
                federal_district = EXCLUDED.federal_district,
                population = EXCLUDED.population,
                foundation_year = EXCLUDED.foundation_year,
                status = EXCLUDED.status,
                old_name = EXCLUDED.old_name,
                date_update = CURRENT_TIMESTAMP
        """)
        conn.commit()

        return len(batch)

try:
    with psycopg2.connect(
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host"),
        port=os.getenv("port"),
        dbname=os.getenv("dbname")
    ) as conn:
        total_rows = 0

        with open(CSV_FILE_PATH, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Пропускаем заголовок

            batch = []
            for row in reader:
                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    processed = process_batch(conn, batch)
                    total_rows += processed
                    print(f"Обработано: {total_rows} строк")
                    batch = []

            if batch:
                processed = process_batch(conn, batch)
                total_rows += processed
                print(f"Обработано: {total_rows} строк (финальный пакет)")

        print(f"Всего загружено строк: {total_rows}")

except Exception as e:
    print(f"Ошибка: {e}")
