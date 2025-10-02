import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SimpleDB
import matplotlib.pyplot as plt
import time
import random
import string

def generate_random_string(length):
    return ''.join(random.choices(string.ascii_letters, k=length))

def test_select_performance():
    print("Тестирование производительности SELECT...")

    db = SimpleDB("test_db")
    db.execute_sql("CREATE TABLE test_table (id INT, name VARCHAR(20))")

    sizes = [100, 500, 1000, 2000, 5000]
    str_times_no_index = []
    int_times_no_index = []
    int_times_with_index = []

    for size in sizes:
        print(f"Тестируем с {size} записями...")

        db.execute_sql("DELETE * FROM test_table")

        for i in range(size):
            name = generate_random_string(10)
            db.execute_sql(f"INSERT INTO test_table VALUES ({i}, '{name}')")

        test_name = generate_random_string(10)
        start_time = time.time()
        db.execute_sql(f"SELECT * FROM test_table WHERE name = '{test_name}'")
        str_time = time.time() - start_time
        str_times_no_index.append(str_time)

        test_id = random.randint(0, size-1)
        start_time = time.time()
        db.execute_sql(f"SELECT * FROM test_table WHERE id = {test_id}")
        int_time_no_idx = time.time() - start_time
        int_times_no_index.append(int_time_no_idx)

        db.execute_sql("CREATE INDEX ON test_table (id)")

        start_time = time.time()
        db.execute_sql(f"SELECT * FROM test_table WHERE id = {test_id}")
        int_time_with_idx = time.time() - start_time
        int_times_with_index.append(int_time_with_idx)

    plt.figure(figsize=(12, 4))

    plt.subplot(1, 3, 1)
    plt.plot(sizes, str_times_no_index, 'b-o', label='Строковый столбец')
    plt.title('SELECT по строковому столбцу')
    plt.xlabel('Количество записей')
    plt.ylabel('Время (сек)')
    plt.grid(True)

    plt.subplot(1, 3, 2)
    plt.plot(sizes, int_times_no_index, 'r-o', label='Без индекса')
    plt.plot(sizes, int_times_with_index, 'g-o', label='С индексом')
    plt.title('SELECT по числовому столбцу')
    plt.xlabel('Количество записей')
    plt.ylabel('Время (сек)')
    plt.legend()
    plt.grid(True)

    plt.subplot(1, 3, 3)
    plt.plot(sizes, int_times_no_index, 'r-o', label='INT без индекса')
    plt.plot(sizes, int_times_with_index, 'g-o', label='INT с индексом')
    plt.plot(sizes, str_times_no_index, 'b-o', label='VARCHAR')
    plt.title('Сравнение всех типов')
    plt.xlabel('Количество записей')
    plt.ylabel('Время (сек)')
    plt.legend()
    plt.grid(True)

    plt.tight_layout()
    plt.savefig('select_performance.png', dpi=150)
    plt.close()

    print("График SELECT сохранен: select_performance.png")

def test_delete_performance():
    print("Тестирование производительности DELETE...")

    db = SimpleDB("test_db")
    sizes = [100, 500, 1000, 2000, 5000]
    str_delete_times = []
    int_delete_times_no_index = []
    int_delete_times_with_index = []

    for size in sizes:
        print(f"Тестируем DELETE с {size} записями...")

        db.execute_sql("DELETE * FROM test_table")
        for i in range(size):
            name = f"name_{i}"
            db.execute_sql(f"INSERT INTO test_table VALUES ({i}, '{name}')")

        start_time = time.time()
        db.execute_sql("DELETE FROM test_table WHERE name = 'name_50'")
        str_delete_time = time.time() - start_time
        str_delete_times.append(str_delete_time)

        db.execute_sql("DELETE * FROM test_table")
        for i in range(size):
            name = f"name_{i}"
            db.execute_sql(f"INSERT INTO test_table VALUES ({i}, '{name}')")

        start_time = time.time()
        db.execute_sql("DELETE FROM test_table WHERE id = 50")
        int_delete_time_no_idx = time.time() - start_time
        int_delete_times_no_index.append(int_delete_time_no_idx)

        db.execute_sql("DELETE * FROM test_table")
        for i in range(size):
            name = f"name_{i}"
            db.execute_sql(f"INSERT INTO test_table VALUES ({i}, '{name}')")

        db.execute_sql("CREATE INDEX ON test_table (id)")

        start_time = time.time()
        db.execute_sql("DELETE FROM test_table WHERE id = 50")
        int_delete_time_with_idx = time.time() - start_time
        int_delete_times_with_index.append(int_delete_time_with_idx)

    plt.figure(figsize=(12, 4))

    plt.subplot(1, 3, 1)
    plt.plot(sizes, str_delete_times, 'b-o', label='Строковый столбец')
    plt.title('DELETE по строковому столбцу')
    plt.xlabel('Количество записей')
    plt.ylabel('Время (сек)')
    plt.grid(True)

    plt.subplot(1, 3, 2)
    plt.plot(sizes, int_delete_times_no_index, 'r-o', label='Без индекса')
    plt.plot(sizes, int_delete_times_with_index, 'g-o', label='С индексом')
    plt.title('DELETE по числовому столбцу')
    plt.xlabel('Количество записей')
    plt.ylabel('Время (сек)')
    plt.legend()
    plt.grid(True)

    plt.subplot(1, 3, 3)
    plt.plot(sizes, int_delete_times_no_index, 'r-o', label='INT без индекса')
    plt.plot(sizes, int_delete_times_with_index, 'g-o', label='С индексом')
    plt.plot(sizes, str_delete_times, 'b-o', label='VARCHAR')
    plt.title('Сравнение всех типов')
    plt.xlabel('Количество записей')
    plt.ylabel('Время (сек)')
    plt.legend()
    plt.grid(True)

    plt.tight_layout()
    plt.savefig('delete_performance.png', dpi=150)
    plt.close()

    print("График DELETE сохранен: delete_performance.png")

def test_insert_performance():
    print("Тестирование производительности INSERT...")

    db = SimpleDB("test_db")
    batch_sizes = [100, 200, 500, 1000, 2000]
    insert_times_no_index = []
    insert_times_with_index = []

    for batch_size in batch_sizes:
        print(f"Тестируем INSERT пачки по {batch_size} записей...")

        db.execute_sql("DELETE * FROM test_table")

        start_time = time.time()
        for i in range(batch_size):
            name = generate_random_string(10)
            db.execute_sql(f"INSERT INTO test_table VALUES ({i}, '{name}')")
        insert_time_no_idx = time.time() - start_time
        insert_times_no_index.append(insert_time_no_idx)

        db.execute_sql("DELETE * FROM test_table")
        db.execute_sql("CREATE INDEX ON test_table (id)")

        start_time = time.time()
        for i in range(batch_size):
            name = generate_random_string(10)
            db.execute_sql(f"INSERT INTO test_table VALUES ({i}, '{name}')")
        insert_time_with_idx = time.time() - start_time
        insert_times_with_index.append(insert_time_with_idx)

    plt.figure(figsize=(10, 6))
    plt.plot(batch_sizes, insert_times_no_index, 'r-o', label='Без индекса')
    plt.plot(batch_sizes, insert_times_with_index, 'g-o', label='С индексом')
    plt.title('Производительность INSERT')
    plt.xlabel('Количество записей в пачке')
    plt.ylabel('Время (сек)')
    plt.legend()
    plt.grid(True)
    plt.savefig('insert_performance.png', dpi=150)
    plt.close()

    print("График INSERT сохранен: insert_performance.png")

def run_all_tests():
    print("Запуск всех тестов СУБД")

    db = SimpleDB("test_db")
    result = db.execute_sql("CREATE TABLE test_table (id INT, name VARCHAR(50))")
    print(f"Создание таблицы: {result}")

    test_select_performance()
    test_delete_performance()
    test_insert_performance()

    print("\nВсе тесты завершены!")
    print("Созданные графики:")
    print("select_performance.png")
    print("delete_performance.png")
    print("insert_performance.png")

if __name__ == "__main__":
    run_all_tests()
