import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.task1_create_tables import create_tables, create_indexes_for_research
from investigations.data_generation_performance import run_generation_research
from investigations.query_performance import run_query_research
from investigations.index_performance import run_index_research
import time

DB_CONFIG = {
    'host': 'localhost',
    'dbname': 'university_db',
    'user': 'postgres',
    'password': '1234'
}

def setup_database():
    print("Настройка базы данных...")
    if not create_tables(**DB_CONFIG):
        print("Ошибка создания таблиц")
        return False

    if not create_indexes_for_research(**DB_CONFIG):
        print("Ошибка создания индексов")
        return False

    print("База данных настроена")
    return True

def run_all_investigations():
    print("Запуск всех исследований производительности")

    if not setup_database():
        return

    start_time = time.time()

    print("\n1. Исследование производительности генерации данных")
    try:
        run_generation_research()
        print("Исследование генерации данных завершено")
    except Exception as e:
        print(f"Ошибка в исследовании генерации: {e}")

    print("\n2. Исследование производительности запросов")
    try:
        run_query_research()
        print("Исследование запросов завершено")
    except Exception as e:
        print(f"Ошибка в исследовании запросов: {e}")

    print("\n3. Исследование эффективности индексов")
    try:
        run_index_research()
        print("Исследование индексов завершено")
    except Exception as e:
        print(f"Ошибка в исследовании индексов: {e}")

    total_time = time.time() - start_time
    print(f"\nВсе исследования завершены за {total_time:.2f} секунд")
    print("Результаты сохранены в папке investigations/results/")

if __name__ == "__main__":
    run_all_investigations()
