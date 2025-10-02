import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.task1_create_tables import db_connection
from lib.task2_data_generation import DataGenerator, generate_groups, generate_courses, generate_students, generate_student_cards, generate_enrollments
from lib.task4_time_plot import plot_simple_graph, measure_query_time
import timeit
import datetime

class QueryPerformanceResearcher:
    def __init__(self, db_config):
        self.db_config = db_config
        self.output_dir = 'results'
        os.makedirs(self.output_dir, exist_ok=True)
        self.gen = DataGenerator(**db_config)

    def prepare_data_for_size(self, target_size):
        self.gen.clear_all_tables()

        num_groups = max(5, target_size // 20)
        num_courses = max(10, target_size // 10)

        groups_data = generate_groups(num_groups)
        group_ids = self.gen.save_groups(groups_data)

        courses_data = generate_courses(num_courses)
        course_ids = self.gen.save_courses(courses_data)

        students_data = generate_students(target_size, group_ids)
        student_ids = self.gen.save_students(students_data)

        cards_data = generate_student_cards(student_ids)
        self.gen.save_student_cards(cards_data)

        enrollments_data = generate_enrollments(student_ids, course_ids)
        self.gen.save_enrollments(enrollments_data)

        return student_ids, course_ids, group_ids

    def research_grouptable_queries(self):
        print("Исследование запросов к GroupTable")
        sizes = [100, 500, 1000, 2000, 5000]

        select_times = []
        insert_times = []
        delete_times = []

        for size in sizes:
            print(f"GroupTable: тестирование с {size} записями...")

            student_ids, course_ids, group_ids = self.prepare_data_for_size(size)

            select_time = measure_query_time(
                "SELECT * FROM GroupTable WHERE created_date > %s",
                (datetime.date.today() - datetime.timedelta(days=180),), **self.db_config
            )
            select_times.append(select_time)

            def insert_groups():
                with db_connection(**self.db_config) as conn:
                    cur = conn.cursor()
                    for i in range(10):
                        cur.execute("INSERT INTO GroupTable (name, created_date) VALUES (%s, %s)",
                                   (f"Temp Group {i}", datetime.date.today()))
                    cur.close()

            insert_time = min(timeit.repeat(insert_groups, repeat=3, number=1))
            insert_times.append(insert_time)

            def delete_temp_groups():
                with db_connection(**self.db_config) as conn:
                    cur = conn.cursor()
                    cur.execute("DELETE FROM GroupTable WHERE name LIKE 'Temp Group%'")
                    cur.close()

            delete_time = min(timeit.repeat(delete_temp_groups, repeat=3, number=1))
            delete_times.append(delete_time)

        plot_simple_graph(sizes, select_times, 'Пункт 5c: Производительность SELECT для GroupTable',
                         'Количество записей в таблице', 'Время выполнения (сек)', f'{self.output_dir}/5c_GroupTable_SELECT')
        plot_simple_graph(sizes, insert_times, 'Пункт 5c: Производительность INSERT для GroupTable',
                         'Количество записей в таблице', 'Время выполнения (сек)', f'{self.output_dir}/5c_GroupTable_INSERT')
        plot_simple_graph(sizes, delete_times, 'Пункт 5c: Производительность DELETE для GroupTable',
                         'Количество записей в таблице', 'Время выполнения (сек)', f'{self.output_dir}/5c_GroupTable_DELETE')

    def research_course_queries(self):
        print("Исследование запросов к Course")
        sizes = [100, 500, 1000, 2000, 5000]

        select_times = []
        insert_times = []
        delete_times = []

        for size in sizes:
            print(f"Course: тестирование с {size} записями...")

            student_ids, course_ids, group_ids = self.prepare_data_for_size(size)

            select_time = measure_query_time(
                "SELECT * FROM Course WHERE credits >= %s",
                (3,), **self.db_config
            )
            select_times.append(select_time)

            def insert_courses():
                with db_connection(**self.db_config) as conn:
                    cur = conn.cursor()
                    for i in range(10):
                        cur.execute("INSERT INTO Course (title, credits, start_date) VALUES (%s, %s, %s)",
                                   (f"Temp Course {i}", 3, datetime.date.today()))
                    cur.close()

            insert_time = min(timeit.repeat(insert_courses, repeat=3, number=1))
            insert_times.append(insert_time)

            def delete_temp_courses():
                with db_connection(**self.db_config) as conn:
                    cur = conn.cursor()
                    cur.execute("DELETE FROM Course WHERE title LIKE 'Temp Course%'")
                    cur.close()

            delete_time = min(timeit.repeat(delete_temp_courses, repeat=3, number=1))
            delete_times.append(delete_time)

        plot_simple_graph(sizes, select_times, 'Пункт 5c: Производительность SELECT для Course',
                         'Количество записей в таблице', 'Время выполнения (сек)', f'{self.output_dir}/5c_Course_SELECT')
        plot_simple_graph(sizes, insert_times, 'Пункт 5c: Производительность INSERT для Course',
                         'Количество записей в таблице', 'Время выполнения (сек)', f'{self.output_dir}/5c_Course_INSERT')
        plot_simple_graph(sizes, delete_times, 'Пункт 5c: Производительность DELETE для Course',
                         'Количество записей в таблице', 'Время выполнения (сек)', f'{self.output_dir}/5c_Course_DELETE')

    def research_student_queries(self):
        print("Исследование запросов к Student")
        sizes = [100, 500, 1000, 2000, 5000]

        select_times = []
        insert_times = []
        delete_times = []

        for size in sizes:
            print(f"Student: тестирование с {size} записями...")

            student_ids, course_ids, group_ids = self.prepare_data_for_size(size)

            select_time = measure_query_time(
                "SELECT * FROM Student WHERE birth_date > %s",
                (datetime.date(2000, 1, 1),), **self.db_config
            )
            select_times.append(select_time)

            def insert_students():
                with db_connection(**self.db_config) as conn:
                    cur = conn.cursor()
                    for i in range(10):
                        cur.execute("INSERT INTO Student (full_name, birth_date, group_id) VALUES (%s, %s, %s)",
                                   (f"Temp Student {i}", datetime.date(2000, 1, 1), group_ids[0] if group_ids else None))
                    cur.close()

            insert_time = min(timeit.repeat(insert_students, repeat=3, number=1))
            insert_times.append(insert_time)

            def delete_temp_students():
                with db_connection(**self.db_config) as conn:
                    cur = conn.cursor()
                    cur.execute("DELETE FROM Student WHERE full_name LIKE 'Temp Student%'")
                    cur.close()

            delete_time = min(timeit.repeat(delete_temp_students, repeat=3, number=1))
            delete_times.append(delete_time)

        plot_simple_graph(sizes, select_times, 'Пункт 5c: Производительность SELECT для Student',
                         'Количество записей в таблице', 'Время выполнения (сек)', f'{self.output_dir}/5c_Student_SELECT')
        plot_simple_graph(sizes, insert_times, 'Пункт 5c: Производительность INSERT для Student',
                         'Количество записей в таблице', 'Время выполнения (сек)', f'{self.output_dir}/5c_Student_INSERT')
        plot_simple_graph(sizes, delete_times, 'Пункт 5c: Производительность DELETE для Student',
                         'Количество записей в таблице', 'Время выполнения (сек)', f'{self.output_dir}/5c_Student_DELETE')

    def research_join_queries(self):
        print("Исследование JOIN запросов")
        sizes = [100, 500, 1000, 2000, 5000]

        one_to_one_times = []
        one_to_many_times = []
        many_to_many_times = []

        for size in sizes:
            print(f"JOIN: тестирование с {size} записями...")

            student_ids, course_ids, group_ids = self.prepare_data_for_size(size)

            one_to_one_time = measure_query_time(
                "SELECT s.full_name, sc.card_number FROM Student s JOIN StudentCard sc ON s.student_id = sc.student_id LIMIT %s",
                (min(100, size),), **self.db_config
            )
            one_to_one_times.append(one_to_one_time)

            one_to_many_time = measure_query_time(
                "SELECT g.name, s.full_name FROM GroupTable g JOIN Student s ON g.group_id = s.group_id LIMIT %s",
                (min(100, size),), **self.db_config
            )
            one_to_many_times.append(one_to_many_time)

            many_to_many_time = measure_query_time(
                "SELECT s.full_name, c.title FROM Student s JOIN Enrollment e ON s.student_id = e.student_id JOIN Course c ON e.course_id = c.course_id LIMIT %s",
                (min(100, size),), **self.db_config
            )
            many_to_many_times.append(many_to_many_time)

        plot_simple_graph(sizes, one_to_one_times, 'Пункт 5c: Производительность JOIN Один-к-Одному (Student-StudentCard)',
                         'Количество записей в таблице', 'Время выполнения (сек)', f'{self.output_dir}/5c_JOIN_OneToOne')
        plot_simple_graph(sizes, one_to_many_times, 'Пункт 5c: Производительность JOIN Один-ко-Многим (Group-Student)',
                         'Количество записей в таблице', 'Время выполнения (сек)', f'{self.output_dir}/5c_JOIN_OneToMany')
        plot_simple_graph(sizes, many_to_many_times, 'Пункт 5c: Производительность JOIN Многие-ко-Многим (Student-Course)',
                         'Количество записей в таблице', 'Время выполнения (сек)', f'{self.output_dir}/5c_JOIN_ManyToMany')

    def run_all_query_research(self):
        self.research_grouptable_queries()
        self.research_course_queries()
        self.research_student_queries()
        self.research_join_queries()

def run_query_research():
    db_config = {
        'host': 'localhost',
        'dbname': 'university_db',
        'user': 'postgres',
        'password': '1234'
    }

    researcher = QueryPerformanceResearcher(db_config)
    researcher.run_all_query_research()

if __name__ == "__main__":
    run_query_research()
