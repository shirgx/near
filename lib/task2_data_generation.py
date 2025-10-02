import random
import datetime
import psycopg2
from contextlib import contextmanager

first_names = ["Иван", "Петр", "Анна", "Ольга", "Мария", "Александр", "Елена", "Дмитрий", "Татьяна", "Николай"]
last_names = ["Иванов", "Петров", "Сидоров", "Смирнов", "Козлов", "Новиков", "Морозов", "Петухов", "Волков", "Соколов"]
patronymics = ["Иванович", "Петрович", "Николаевич", "Александрович", "Дмитриевич", "Ивановна", "Петровна", "Николаевна", "Александровна", "Дмитриевна"]

course_titles = ["Математика", "Физика", "Химия", "Биология", "История", "Литература", "Информатика", "Английский язык", "Философия", "Экономика"]

def generate_groups(n):
    groups = []
    for i in range(n):
        name = f"Группа {i + 1}"
        days_ago = random.randint(0, 365)
        date = datetime.date.today() - datetime.timedelta(days=days_ago)
        groups.append((name, date))
    return groups

def generate_courses(n):
    courses = []
    for i in range(n):
        title = random.choice(course_titles) + f" {i + 1}"
        credits = random.randint(1, 10)
        days_ahead = random.randint(0, 180)
        start_date = datetime.date.today() + datetime.timedelta(days=days_ahead)
        courses.append((title, credits, start_date))
    return courses

def generate_students(n, group_ids):
    students = []
    for i in range(n):
        first = random.choice(first_names)
        last = random.choice(last_names)

        if first in ["Иван", "Петр", "Александр", "Дмитрий", "Николай"]:
            patr_list = [p for p in patronymics if p.endswith("ович")]
        else:
            patr_list = [p for p in patronymics if p.endswith("овна")]

        patr = random.choice(patr_list)
        full_name = f"{last} {first} {patr}"

        birth_year = random.randint(1995, 2005)
        birth_month = random.randint(1, 12)
        birth_day = random.randint(1, 28)
        birth_date = datetime.date(birth_year, birth_month, birth_day)

        group_id = random.choice(group_ids) if group_ids else None
        students.append((full_name, birth_date, group_id))

    return students

def generate_student_cards(student_ids):
    cards = []
    used_numbers = set()

    for student_id in student_ids:
        while True:
            number = random.randint(100000, 999999)
            card_number = f"ST{number}"
            if card_number not in used_numbers:
                used_numbers.add(card_number)
                break

        days_ago = random.randint(0, 730)
        issue_date = datetime.date.today() - datetime.timedelta(days=days_ago)
        cards.append((student_id, card_number, issue_date))

    return cards

def generate_enrollments(student_ids, course_ids):
    enrollments = []

    for student_id in student_ids:
        num_courses = random.randint(1, min(len(course_ids), 5))
        selected = random.sample(course_ids, num_courses)

        for course_id in selected:
            if random.random() < 0.7:
                days_ago = random.randint(0, 180)
                enroll_date = datetime.date.today() - datetime.timedelta(days=days_ago)

                if random.random() < 0.8:
                    grade = random.randint(2, 5)
                else:
                    grade = None

                enrollments.append((student_id, course_id, enroll_date, grade))

    return enrollments

@contextmanager
def db_connection(host, dbname, user, password):
    conn = None
    try:
        conn = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password
        )
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()

class DataGenerator:
    def __init__(self, host, dbname, user, password):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password

    def save_groups(self, groups_data):
        group_ids = []
        with db_connection(self.host, self.dbname, self.user, self.password) as conn:
            cur = conn.cursor()
            for name, created_date in groups_data:
                cur.execute(
                    "INSERT INTO GroupTable (name, created_date) VALUES (%s, %s) RETURNING group_id",
                    (name, created_date)
                )
                group_id = cur.fetchone()[0]
                group_ids.append(group_id)
            cur.close()
        return group_ids

    def save_courses(self, courses_data):
        course_ids = []
        with db_connection(self.host, self.dbname, self.user, self.password) as conn:
            cur = conn.cursor()
            for title, credits, start_date in courses_data:
                cur.execute(
                    "INSERT INTO Course (title, credits, start_date) VALUES (%s, %s, %s) RETURNING course_id",
                    (title, credits, start_date)
                )
                course_id = cur.fetchone()[0]
                course_ids.append(course_id)
            cur.close()
        return course_ids

    def save_students(self, students_data):
        student_ids = []
        with db_connection(self.host, self.dbname, self.user, self.password) as conn:
            cur = conn.cursor()
            for full_name, birth_date, group_id in students_data:
                cur.execute(
                    "INSERT INTO Student (full_name, birth_date, group_id) VALUES (%s, %s, %s) RETURNING student_id",
                    (full_name, birth_date, group_id)
                )
                student_id = cur.fetchone()[0]
                student_ids.append(student_id)
            cur.close()
        return student_ids

    def save_student_cards(self, cards_data):
        with db_connection(self.host, self.dbname, self.user, self.password) as conn:
            cur = conn.cursor()
            for student_id, card_number, issue_date in cards_data:
                cur.execute(
                    "INSERT INTO StudentCard (student_id, card_number, issue_date) VALUES (%s, %s, %s)",
                    (student_id, card_number, issue_date)
                )
            cur.close()

    def save_enrollments(self, enrollments_data):
        with db_connection(self.host, self.dbname, self.user, self.password) as conn:
            cur = conn.cursor()
            for student_id, course_id, enroll_date, grade in enrollments_data:
                cur.execute(
                    "INSERT INTO Enrollment (student_id, course_id, enroll_date, grade) VALUES (%s, %s, %s, %s)",
                    (student_id, course_id, enroll_date, grade)
                )
            cur.close()

    def clear_table(self, table_name):
        with db_connection(self.host, self.dbname, self.user, self.password) as conn:
            cur = conn.cursor()
            cur.execute(f"DELETE FROM {table_name}")
            cur.close()

    def clear_all_tables(self):
        tables = ["Enrollment", "StudentCard", "Student", "Course", "GroupTable"]
        for table in tables:
            self.clear_table(table)
