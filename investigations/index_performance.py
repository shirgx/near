import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.task1_create_tables import create_test_tables_for_index_research, db_connection
from lib.task2_data_generation import generate_groups, generate_courses, generate_students, DataGenerator
from lib.task4_time_plot import plot_multiple_series, measure_query_time
import timeit
import random
import datetime

class IndexPerformanceResearcher:
    def __init__(self, db_config):
        self.db_config = db_config
        self.output_dir = 'results'
        os.makedirs(self.output_dir, exist_ok=True)

    def prepare_test_data(self, size):
        gen = DataGenerator(**self.db_config)

        groups_data = generate_groups(max(5, size // 20))
        group_ids = gen.save_groups(groups_data)

        students_data = generate_students(size, group_ids)
        student_ids = gen.save_students(students_data)

        courses_data = generate_courses(max(10, size // 10))
        course_ids = gen.save_courses(courses_data)

        return student_ids, course_ids

    def populate_test_tables(self, sizes):
        results = {}

        for size in sizes:
            print(f"Подготовка данных для {size} записей...")

            student_ids, course_ids = self.prepare_test_data(size)

            with db_connection(**self.db_config) as conn:
                cur = conn.cursor()

                cur.execute("SELECT full_name, birth_date, group_id FROM Student LIMIT %s", (size,))
                student_data = cur.fetchall()

                cur.execute("SELECT title, credits, start_date FROM Course LIMIT %s", (size,))
                course_data = cur.fetchall()

                try:
                    cur.execute("DELETE FROM Student_T1")
                    cur.execute("DELETE FROM Student_T2")
                    cur.execute("DELETE FROM Student_T3")
                    cur.execute("DELETE FROM Student_T4")
                    cur.execute("DELETE FROM Course_T5")
                    cur.execute("DELETE FROM Course_T6")
                except:
                    pass

                for i, (name, birth, group_id) in enumerate(student_data):
                    student_id = i + 1
                    try:
                        cur.execute("INSERT INTO Student_T1 (student_id, full_name, birth_date, group_id) VALUES (%s, %s, %s, %s)",
                                   (student_id, name, birth, group_id))
                        cur.execute("INSERT INTO Student_T2 (student_id, full_name, birth_date, group_id) VALUES (%s, %s, %s, %s)",
                                   (student_id, name, birth, group_id))
                        cur.execute("INSERT INTO Student_T3 (student_id, full_name, birth_date, group_id) VALUES (%s, %s, %s, %s)",
                                   (student_id, name, birth, group_id))
                        cur.execute("INSERT INTO Student_T4 (student_id, full_name, birth_date, group_id) VALUES (%s, %s, %s, %s)",
                                   (student_id, name, birth, group_id))
                    except Exception as e:
                        print(f"Ошибка вставки студента {student_id}: {e}")
                        break

                for i, (title, credits, start_date) in enumerate(course_data):
                    course_id = i + 1
                    try:
                        cur.execute("INSERT INTO Course_T5 (course_id, title, credits, start_date) VALUES (%s, %s, %s, %s)",
                                   (course_id, title, credits, start_date))
                        cur.execute("INSERT INTO Course_T6 (course_id, title, credits, start_date) VALUES (%s, %s, %s, %s)",
                                   (course_id, title, credits, start_date))
                    except Exception as e:
                        print(f"Ошибка вставки курса {course_id}: {e}")
                        break

                cur.close()

            results[size] = {'students': len(student_data), 'courses': len(course_data)}

        return results

    def research_primary_key_performance(self):
        print("Исследование производительности первичного ключа")

        sizes = [100, 500, 1000, 2000, 5000]
        self.populate_test_tables(sizes)

        equality_times_t1 = []
        equality_times_t2 = []
        inequality_times_t1 = []
        inequality_times_t2 = []
        insert_times_t1 = []
        insert_times_t2 = []

        for size in sizes:
            print(f"Тестирование с {size} записями...")

            test_id = random.randint(1, size)

            eq_time_t1 = measure_query_time(
                "SELECT * FROM Student_T1 WHERE student_id = %s",
                (test_id,), **self.db_config
            )
            equality_times_t1.append(eq_time_t1)

            eq_time_t2 = measure_query_time(
                "SELECT * FROM Student_T2 WHERE student_id = %s",
                (test_id,), **self.db_config
            )
            equality_times_t2.append(eq_time_t2)

            ineq_time_t1 = measure_query_time(
                "SELECT * FROM Student_T1 WHERE student_id < %s",
                (test_id,), **self.db_config
            )
            inequality_times_t1.append(ineq_time_t1)

            ineq_time_t2 = measure_query_time(
                "SELECT * FROM Student_T2 WHERE student_id < %s",
                (test_id,), **self.db_config
            )
            inequality_times_t2.append(ineq_time_t2)

            def insert_t1():
                with db_connection(**self.db_config) as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT COALESCE(MAX(student_id), 0) FROM Student_T1")
                    max_id = cur.fetchone()[0]

                    for i in range(10):
                        cur.execute("INSERT INTO Student_T1 (student_id, full_name, birth_date) VALUES (%s, %s, %s)",
                                   (max_id + i + 1, f"Test User {i}", datetime.date.today()))
                    cur.execute("DELETE FROM Student_T1 WHERE full_name LIKE 'Test User%'")
                    cur.close()

            def insert_t2():
                with db_connection(**self.db_config) as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT COALESCE(MAX(student_id), 0) FROM Student_T2")
                    max_id = cur.fetchone()[0]

                    for i in range(10):
                        cur.execute("INSERT INTO Student_T2 (student_id, full_name, birth_date) VALUES (%s, %s, %s)",
                                   (max_id + i + 1, f"Test User {i}", datetime.date.today()))
                    cur.execute("DELETE FROM Student_T2 WHERE full_name LIKE 'Test User%'")
                    cur.close()

            ins_time_t1 = min(timeit.repeat(insert_t1, repeat=3, number=1))
            insert_times_t1.append(ins_time_t1)

            ins_time_t2 = min(timeit.repeat(insert_t2, repeat=3, number=1))
            insert_times_t2.append(ins_time_t2)

        plot_multiple_series({
            'T1 (с первичным ключом)': (sizes, equality_times_t1),
            'T2 (без первичного ключа)': (sizes, equality_times_t2)
        }, 'Пункт 6a: Производительность SELECT по первичному ключу (равенство)',
           'Количество записей в таблице', 'Время выполнения (сек)',
           f'{self.output_dir}/6a_pk_select_equality')

        plot_multiple_series({
            'T1 (с первичным ключом)': (sizes, inequality_times_t1),
            'T2 (без первичного ключа)': (sizes, inequality_times_t2)
        }, 'Пункт 6a: Производительность SELECT по первичному ключу (неравенство)',
           'Количество записей в таблице', 'Время выполнения (сек)',
           f'{self.output_dir}/6a_pk_select_inequality')

        plot_multiple_series({
            'T1 (с первичным ключом)': (sizes, insert_times_t1),
            'T2 (без первичного ключа)': (sizes, insert_times_t2)
        }, 'Пункт 6a: Производительность INSERT с первичным ключом',
           'Количество записей в таблице', 'Время выполнения (сек)',
           f'{self.output_dir}/6a_pk_insert')

    def research_string_index_performance(self):
        print("Исследование производительности строкового индекса")

        sizes = [100, 500, 1000, 2000, 5000]

        equality_times_t3 = []
        equality_times_t4 = []
        like_start_times_t3 = []
        like_start_times_t4 = []
        like_contain_times_t3 = []
        like_contain_times_t4 = []
        insert_times_t3 = []
        insert_times_t4 = []

        for size in sizes:
            print(f"Тестирование строкового индекса с {size} записями...")

            with db_connection(**self.db_config) as conn:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM Student_T3")
                count = cur.fetchone()[0]

                if count == 0:
                    cur.execute("SELECT full_name FROM Student ORDER BY RANDOM() LIMIT 1")
                    result = cur.fetchone()
                    test_name = result[0] if result else "Иванов Иван Иванович"
                else:
                    cur.execute("SELECT full_name FROM Student_T3 ORDER BY RANDOM() LIMIT 1")
                    test_name = cur.fetchone()[0]
                cur.close()

            eq_time_t3 = measure_query_time(
                "SELECT * FROM Student_T3 WHERE full_name = %s",
                (test_name,), **self.db_config
            )
            equality_times_t3.append(eq_time_t3)

            eq_time_t4 = measure_query_time(
                "SELECT * FROM Student_T4 WHERE full_name = %s",
                (test_name,), **self.db_config
            )
            equality_times_t4.append(eq_time_t4)

            like_start_t3 = measure_query_time(
                "SELECT * FROM Student_T3 WHERE full_name LIKE %s",
                (test_name[:3] + '%',), **self.db_config
            )
            like_start_times_t3.append(like_start_t3)

            like_start_t4 = measure_query_time(
                "SELECT * FROM Student_T4 WHERE full_name LIKE %s",
                (test_name[:3] + '%',), **self.db_config
            )
            like_start_times_t4.append(like_start_t4)

            like_contain_t3 = measure_query_time(
                "SELECT * FROM Student_T3 WHERE full_name LIKE %s",
                ('%' + test_name[2:5] + '%',), **self.db_config
            )
            like_contain_times_t3.append(like_contain_t3)

            like_contain_t4 = measure_query_time(
                "SELECT * FROM Student_T4 WHERE full_name LIKE %s",
                ('%' + test_name[2:5] + '%',), **self.db_config
            )
            like_contain_times_t4.append(like_contain_t4)

            def insert_t3():
                with db_connection(**self.db_config) as conn:
                    cur = conn.cursor()
                    for i in range(10):
                        cur.execute("INSERT INTO Student_T3 (full_name, birth_date) VALUES (%s, %s)",
                                   (f"Test String User {i}", datetime.date.today()))
                    cur.execute("DELETE FROM Student_T3 WHERE full_name LIKE 'Test String User%'")
                    cur.close()

            def insert_t4():
                with db_connection(**self.db_config) as conn:
                    cur = conn.cursor()
                    for i in range(10):
                        cur.execute("INSERT INTO Student_T4 (full_name, birth_date) VALUES (%s, %s)",
                                   (f"Test String User {i}", datetime.date.today()))
                    cur.execute("DELETE FROM Student_T4 WHERE full_name LIKE 'Test String User%'")
                    cur.close()

            ins_time_t3 = min(timeit.repeat(insert_t3, repeat=3, number=1))
            insert_times_t3.append(ins_time_t3)

            ins_time_t4 = min(timeit.repeat(insert_t4, repeat=3, number=1))
            insert_times_t4.append(ins_time_t4)

        plot_multiple_series({
            'T3 (со строковым индексом)': (sizes, equality_times_t3),
            'T4 (без строкового индекса)': (sizes, equality_times_t4)
        }, 'Пункт 6b: Производительность SELECT по строке (равенство)',
           'Количество записей в таблице', 'Время выполнения (сек)',
           f'{self.output_dir}/6b_string_select_equality')

        plot_multiple_series({
            'T3 (со строковым индексом)': (sizes, like_start_times_t3),
            'T4 (без строкового индекса)': (sizes, like_start_times_t4)
        }, 'Пункт 6b: Производительность SELECT по строке (LIKE с начала)',
           'Количество записей в таблице', 'Время выполнения (сек)',
           f'{self.output_dir}/6b_string_select_like_start')

        plot_multiple_series({
            'T3 (со строковым индексом)': (sizes, like_contain_times_t3),
            'T4 (без строкового индекса)': (sizes, like_contain_times_t4)
        }, 'Пункт 6b: Производительность SELECT по строке (LIKE содержание)',
           'Количество записей в таблице', 'Время выполнения (сек)',
           f'{self.output_dir}/6b_string_select_like_contain')

        plot_multiple_series({
            'T3 (со строковым индексом)': (sizes, insert_times_t3),
            'T4 (без строкового индекса)': (sizes, insert_times_t4)
        }, 'Пункт 6b: Производительность INSERT со строковым индексом',
           'Количество записей в таблице', 'Время выполнения (сек)',
           f'{self.output_dir}/6b_string_insert')

    def research_fulltext_index_performance(self):
        print("Исследование производительности полнотекстового индекса")

        sizes = [100, 500, 1000, 2000, 5000]

        single_word_times_t5 = []
        single_word_times_t6 = []
        multiple_words_times_t5 = []
        multiple_words_times_t6 = []
        insert_times_t5 = []
        insert_times_t6 = []

        for size in sizes:
            print(f"Тестирование полнотекстового индекса с {size} записями...")

            single_t5 = measure_query_time(
                "SELECT * FROM Course_T5 WHERE to_tsvector('russian', title) @@ to_tsquery('russian', %s)",
                ('Математика',), **self.db_config
            )
            single_word_times_t5.append(single_t5)

            single_t6 = measure_query_time(
                "SELECT * FROM Course_T6 WHERE title ILIKE %s",
                ('%Математика%',), **self.db_config
            )
            single_word_times_t6.append(single_t6)

            multiple_t5 = measure_query_time(
                "SELECT * FROM Course_T5 WHERE to_tsvector('russian', title) @@ to_tsquery('russian', %s)",
                ('Математика & курс',), **self.db_config
            )
            multiple_words_times_t5.append(multiple_t5)

            multiple_t6 = measure_query_time(
                "SELECT * FROM Course_T6 WHERE title ILIKE %s AND title ILIKE %s",
                ('%Математика%', '%курс%'), **self.db_config
            )
            multiple_words_times_t6.append(multiple_t6)

            def insert_t5():
                with db_connection(**self.db_config) as conn:
                    cur = conn.cursor()
                    for i in range(10):
                        cur.execute("INSERT INTO Course_T5 (title, credits, start_date) VALUES (%s, %s, %s)",
                                   (f"Тестовый курс математики номер {i}", 3, datetime.date.today()))
                    cur.execute("DELETE FROM Course_T5 WHERE title LIKE 'Тестовый курс%'")
                    cur.close()

            def insert_t6():
                with db_connection(**self.db_config) as conn:
                    cur = conn.cursor()
                    for i in range(10):
                        cur.execute("INSERT INTO Course_T6 (title, credits, start_date) VALUES (%s, %s, %s)",
                                   (f"Тестовый курс математики номер {i}", 3, datetime.date.today()))
                    cur.execute("DELETE FROM Course_T6 WHERE title LIKE 'Тестовый курс%'")
                    cur.close()

            ins_time_t5 = min(timeit.repeat(insert_t5, repeat=3, number=1))
            insert_times_t5.append(ins_time_t5)

            ins_time_t6 = min(timeit.repeat(insert_t6, repeat=3, number=1))
            insert_times_t6.append(ins_time_t6)

        plot_multiple_series({
            'T5 (с полнотекстовым индексом)': (sizes, single_word_times_t5),
            'T6 (без полнотекстового индекса)': (sizes, single_word_times_t6)
        }, 'Пункт 6c: Производительность полнотекстового поиска (одно слово)',
           'Количество записей в таблице', 'Время выполнения (сек)',
           f'{self.output_dir}/6c_fts_single_word')

        plot_multiple_series({
            'T5 (с полнотекстовым индексом)': (sizes, multiple_words_times_t5),
            'T6 (без полнотекстового индекса)': (sizes, multiple_words_times_t6)
        }, 'Пункт 6c: Производительность полнотекстового поиска (несколько слов)',
           'Количество записей в таблице', 'Время выполнения (сек)',
           f'{self.output_dir}/6c_fts_multiple_words')

        plot_multiple_series({
            'T5 (с полнотекстовым индексом)': (sizes, insert_times_t5),
            'T6 (без полнотекстового индекса)': (sizes, insert_times_t6)
        }, 'Пункт 6c: Производительность INSERT с полнотекстовым индексом',
           'Количество записей в таблице', 'Время выполнения (сек)',
           f'{self.output_dir}/6c_fts_insert')

    def run_all_index_research(self):
        print("Создание тестовых таблиц...")
        create_test_tables_for_index_research(**self.db_config)

        self.research_primary_key_performance()
        self.research_string_index_performance()
        self.research_fulltext_index_performance()

def run_index_research():
    db_config = {
        'host': 'localhost',
        'dbname': 'university_db',
        'user': 'postgres',
        'password': '1234'
    }

    researcher = IndexPerformanceResearcher(db_config)
    researcher.run_all_index_research()

if __name__ == "__main__":
    run_index_research()
