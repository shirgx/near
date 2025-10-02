import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.task1_create_tables import create_tables, drop_all_tables, db_connection, create_database_if_not_exists
from lib.task2_data_generation import DataGenerator, generate_groups, generate_courses, generate_students
from lib.task3_db_clone_backup import DatabaseManager
from lib.task4_time_plot import measure_query_time, plot_simple_graph
import datetime

class FunctionalityTester:
    def __init__(self):
        self.db_config = {
            'host': 'localhost',
            'dbname': 'test_university_db',
            'user': 'postgres',
            'password': '1234'
        }
        self.test_results = []

        self.setup_database()

    def setup_database(self):
        try:
            create_database_if_not_exists(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                dbname=self.db_config['dbname']
            )
        except Exception as e:
            print(f"Ошибка при настройке базы данных: {e}")

    def log_test(self, test_name, success, message=""):
        status = "PASS" if success else "FAIL"
        result = f"[{status}] {test_name}"
        if message:
            result += f": {message}"
        print(result)
        self.test_results.append((test_name, success, message))

    def test_database_connection(self):
        try:
            with db_connection(**self.db_config) as conn:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                result = cur.fetchone()
                cur.close()
                success = result[0] == 1
                self.log_test("Database Connection", success)
                return success
        except Exception as e:
            self.log_test("Database Connection", False, str(e))
            return False

    def test_table_creation(self):
        try:
            drop_all_tables(**self.db_config)
            success = create_tables(**self.db_config)
            self.log_test("Table Creation", success)
            return success
        except Exception as e:
            self.log_test("Table Creation", False, str(e))
            return False

    def test_data_generation(self):
        try:
            groups = generate_groups(5)
            courses = generate_courses(10)
            students = generate_students(20, [1, 2, 3, 4, 5])

            success = (len(groups) == 5 and len(courses) == 10 and len(students) == 20)
            self.log_test("Data Generation Functions", success)
            return success
        except Exception as e:
            self.log_test("Data Generation Functions", False, str(e))
            return False

    def test_data_saving(self):
        try:
            gen = DataGenerator(**self.db_config)

            groups_data = generate_groups(3)
            group_ids = gen.save_groups(groups_data)

            students_data = generate_students(10, group_ids)
            student_ids = gen.save_students(students_data)

            success = len(group_ids) == 3 and len(student_ids) == 10
            self.log_test("Data Saving to Database", success)
            return success
        except Exception as e:
            self.log_test("Data Saving to Database", False, str(e))
            return False

    def test_query_measurement(self):
        try:
            time_taken = measure_query_time(
                "SELECT COUNT(*) FROM Student",
                (), **self.db_config
            )
            success = time_taken > 0
            self.log_test("Query Time Measurement", success, f"Measured: {time_taken:.6f}s")
            return success
        except Exception as e:
            self.log_test("Query Time Measurement", False, str(e))
            return False

    def test_database_manager(self):
        try:
            db_manager = DatabaseManager(**self.db_config)

            sandbox_created = db_manager.create_sandbox("test_sandbox")
            if not sandbox_created:
                self.log_test("Database Manager", False, "Failed to create sandbox")
                return False

            schema_cloned = db_manager.clone_schema_to_sandbox("test_sandbox")
            if not schema_cloned:
                self.log_test("Database Manager", False, "Failed to clone schema")
                return False

            sandbox_dropped = db_manager.drop_sandbox("test_sandbox")
            success = sandbox_dropped

            self.log_test("Database Manager (Sandbox)", success)
            return success
        except Exception as e:
            self.log_test("Database Manager (Sandbox)", False, str(e))
            return False

    def test_backup_restore(self):
        try:
            db_manager = DatabaseManager(**self.db_config)
            backup_dir = "test_backup"

            backup_success = db_manager.backup_all_tables(backup_dir)
            if not backup_success:
                self.log_test("Backup and Restore", False, "Backup failed")
                return False

            restore_success = db_manager.restore_all_tables(backup_dir)
            success = restore_success

            import shutil
            if os.path.exists(backup_dir):
                shutil.rmtree(backup_dir)

            self.log_test("Backup and Restore", success)
            return success
        except Exception as e:
            self.log_test("Backup and Restore", False, str(e))
            return False

    def test_context_managers(self):
        try:
            with db_connection(**self.db_config) as conn:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM Student")
                count = cur.fetchone()[0]
                cur.close()

            success = count >= 0
            self.log_test("Context Manager Usage", success, f"Student count: {count}")
            return success
        except Exception as e:
            self.log_test("Context Manager Usage", False, str(e))
            return False

    def test_data_constraints(self):
        try:
            groups = generate_groups(5)
            students = generate_students(10, [1, 2, 3, 4, 5])

            all_names_valid = all(len(student[0].split()) == 3 for student in students)

            all_dates_valid = all(isinstance(group[1], datetime.date) for group in groups)
            all_dates_valid &= all(isinstance(student[1], datetime.date) for student in students)

            success = all_names_valid and all_dates_valid
            self.log_test("Data Constraints Validation", success,
                         f"Names: {all_names_valid}, Dates: {all_dates_valid}")
            return success
        except Exception as e:
            self.log_test("Data Constraints Validation", False, str(e))
            return False

    def run_all_tests(self):
        print("Запуск функциональных тестов\n")

        test_methods = [
            self.test_database_connection,
            self.test_table_creation,
            self.test_data_generation,
            self.test_data_saving,
            self.test_query_measurement,
            self.test_database_manager,
            self.test_backup_restore,
            self.test_context_managers,
            self.test_data_constraints
        ]

        passed = 0
        total = len(test_methods)

        for test_method in test_methods:
            if test_method():
                passed += 1

        print(f"\nТестирование завершено: {passed}/{total} тестов прошли успешно")

        if passed == total:
            print("Все функции работают корректно!")
        else:
            print("Обнаружены проблемы в следующих компонентах:")
            for test_name, success, message in self.test_results:
                if not success:
                    print(f"  - {test_name}: {message}")

        return passed == total

if __name__ == "__main__":
    tester = FunctionalityTester()
    tester.run_all_tests()
