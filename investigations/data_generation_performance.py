import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.task2_data_generation import (
    generate_groups, generate_courses, generate_students,
    generate_student_cards, generate_enrollments
)
from lib.task4_time_plot import plot_multiple_series
import timeit

class DataGenerationPerformanceResearcher:
    def __init__(self):
        self.output_dir = 'results'
        os.makedirs(self.output_dir, exist_ok=True)

    def research_data_generation_performance(self):
        print("Исследование производительности генерации данных")

        sizes = [100, 500, 1000, 2000, 5000]

        group_times = []
        course_times = []
        student_times_no_fk = []
        student_times_with_fk = []
        studentcard_times = []
        enrollment_times = []
        linking_tables_times = []
        fk_impact_times = []

        for size in sizes:
            print(f"Тестирование генерации с {size} записями...")

            group_time = min(timeit.repeat(
                lambda s=size: generate_groups(s),
                repeat=3, number=1
            ))
            group_times.append(group_time)

            course_time = min(timeit.repeat(
                lambda s=size: generate_courses(s),
                repeat=3, number=1
            ))
            course_times.append(course_time)

            student_no_fk_time = min(timeit.repeat(
                lambda s=size: generate_students(s, []),
                repeat=3, number=1
            ))
            student_times_no_fk.append(student_no_fk_time)

            def generate_students_with_groups():
                groups = generate_groups(max(5, size // 20))
                group_ids = list(range(1, len(groups) + 1))
                return generate_students(size, group_ids)

            student_with_fk_time = min(timeit.repeat(
                generate_students_with_groups,
                repeat=3, number=1
            ))
            student_times_with_fk.append(student_with_fk_time)

            student_ids = list(range(1, size + 1))
            studentcard_time = min(timeit.repeat(
                lambda ids=student_ids: generate_student_cards(ids),
                repeat=3, number=1
            ))
            studentcard_times.append(studentcard_time)

            def generate_full_enrollment():
                student_ids = list(range(1, size + 1))
                course_ids = list(range(1, max(10, size // 10) + 1))
                return generate_enrollments(student_ids, course_ids)

            enrollment_time = min(timeit.repeat(
                generate_full_enrollment,
                repeat=3, number=1
            ))
            enrollment_times.append(enrollment_time)

            def generate_linking_tables():
                groups = generate_groups(max(5, size // 20))
                students = generate_students(size, list(range(1, len(groups) + 1)))
                courses = generate_courses(max(10, size // 10))
                return groups, students, courses

            linking_time = min(timeit.repeat(
                generate_linking_tables,
                repeat=3, number=1
            ))
            linking_tables_times.append(linking_time)

            def generate_with_fk_impact():
                groups = generate_groups(max(5, size // 20))
                group_ids = list(range(1, len(groups) + 1))
                students = generate_students(size, group_ids)
                return groups, students

            fk_impact_time = min(timeit.repeat(
                generate_with_fk_impact,
                repeat=3, number=1
            ))
            fk_impact_times.append(fk_impact_time)

        plot_multiple_series({
            'GroupTable': (sizes, group_times),
            'Course': (sizes, course_times),
            'Student без FK': (sizes, student_times_no_fk),
            'Student с FK': (sizes, student_times_with_fk),
            'StudentCard (1:1)': (sizes, studentcard_times),
            'Enrollment (M:N)': (sizes, enrollment_times)
        }, 'Пункт 5b: Производительность генерации данных по таблицам',
           'Количество записей', 'Время выполнения (сек)',
           f'{self.output_dir}/5b_data_generation_performance')

        plot_multiple_series({
            'Связанные таблицы (Groups+Students)': (sizes, linking_tables_times),
            'Влияние FK (Groups->Students)': (sizes, fk_impact_times)
        }, 'Пункт 5b: Производительность генерации связанных таблиц',
           'Количество записей', 'Время выполнения (сек)',
           f'{self.output_dir}/5b_linking_tables_generation')

        plot_multiple_series({
            'Без внешних ключей': (sizes, student_times_no_fk),
            'С внешними ключами': (sizes, fk_impact_times)
        }, 'Пункт 5b: Влияние внешних ключей на время генерации',
           'Количество записей', 'Время выполнения (сек)',
           f'{self.output_dir}/5b_foreign_keys_impact')

def run_generation_research():
    researcher = DataGenerationPerformanceResearcher()
    researcher.research_data_generation_performance()

if __name__ == "__main__":
    run_generation_research()
