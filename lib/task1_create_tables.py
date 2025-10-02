import psycopg2
from contextlib import contextmanager

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

def create_database_if_not_exists(host, user, password, dbname):
    try:
        conn = psycopg2.connect(
            host=host,
            dbname='postgres',
            user=user,
            password=password
        )
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
        exists = cur.fetchone()

        if not exists:
            cur.execute(f'CREATE DATABASE "{dbname}"')
            print(f"База данных '{dbname}' успешно создана!")
        else:
            print(f"База данных '{dbname}' уже существует.")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"Ошибка при создании базы данных: {e}")
        return False

def create_tables(host, dbname, user, password):
    try:
        with db_connection(host, dbname, user, password) as conn:
            cur = conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS GroupTable (
                    group_id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    created_date DATE
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS Course (
                    course_id SERIAL PRIMARY KEY,
                    title VARCHAR(100) NOT NULL,
                    credits INTEGER,
                    start_date DATE
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS Student (
                    student_id SERIAL PRIMARY KEY,
                    full_name VARCHAR(100) NOT NULL,
                    birth_date DATE,
                    group_id INTEGER REFERENCES GroupTable(group_id)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS StudentCard (
                    student_id INTEGER PRIMARY KEY REFERENCES Student(student_id),
                    card_number VARCHAR(20) UNIQUE NOT NULL,
                    issue_date DATE
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS Enrollment (
                    student_id INTEGER REFERENCES Student(student_id),
                    course_id INTEGER REFERENCES Course(course_id),
                    enroll_date DATE,
                    grade INTEGER,
                    PRIMARY KEY(student_id, course_id)
                )
            """)

            cur.close()
            print("Таблицы успешно созданы!")
            return True

    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")
        return False

def drop_all_tables(host, dbname, user, password):
    try:
        with db_connection(host, dbname, user, password) as conn:
            cur = conn.cursor()
            cur.execute("""
                DROP TABLE IF EXISTS Enrollment CASCADE;
                DROP TABLE IF EXISTS StudentCard CASCADE;
                DROP TABLE IF EXISTS Student CASCADE;
                DROP TABLE IF EXISTS Course CASCADE;
                DROP TABLE IF EXISTS GroupTable CASCADE;
            """)
            cur.close()
            print("Все таблицы удалены!")
            return True

    except Exception as e:
        print(f"Ошибка при удалении таблиц: {e}")
        return False

def create_indexes_for_research(host, dbname, user, password):
    try:
        with db_connection(host, dbname, user, password) as conn:
            cur = conn.cursor()

            cur.execute("CREATE INDEX IF NOT EXISTS idx_student_name ON Student (full_name)")

            cur.execute("CREATE INDEX IF NOT EXISTS idx_course_title_fts ON Course USING gin(to_tsvector('russian', title))")

            cur.close()
            print("Индексы для исследований созданы!")
            return True

    except Exception as e:
        print(f"Ошибка при создании индексов: {e}")
        return False

def create_test_tables_for_index_research(host, dbname, user, password):
    try:
        with db_connection(host, dbname, user, password) as conn:
            cur = conn.cursor()

            cur.execute("DROP INDEX IF EXISTS idx_t3_name CASCADE")
            cur.execute("DROP INDEX IF EXISTS idx_t5_title_fts CASCADE")

            cur.execute("DROP TABLE IF EXISTS Student_T1 CASCADE")
            cur.execute("DROP TABLE IF EXISTS Student_T2 CASCADE")
            cur.execute("DROP TABLE IF EXISTS Student_T3 CASCADE")
            cur.execute("DROP TABLE IF EXISTS Student_T4 CASCADE")
            cur.execute("DROP TABLE IF EXISTS Course_T5 CASCADE")
            cur.execute("DROP TABLE IF EXISTS Course_T6 CASCADE")

            cur.execute("""
                CREATE TABLE Student_T1 (
                    student_id INTEGER PRIMARY KEY,
                    full_name VARCHAR(100),
                    birth_date DATE,
                    group_id INTEGER
                )
            """)

            cur.execute("""
                CREATE TABLE Student_T2 (
                    student_id INTEGER,
                    full_name VARCHAR(100),
                    birth_date DATE,
                    group_id INTEGER
                )
            """)

            cur.execute("""
                CREATE TABLE Student_T3 (
                    student_id INTEGER,
                    full_name VARCHAR(100),
                    birth_date DATE,
                    group_id INTEGER
                )
            """)
            cur.execute("CREATE INDEX idx_t3_name ON Student_T3 (full_name)")

            cur.execute("""
                CREATE TABLE Student_T4 (
                    student_id INTEGER,
                    full_name VARCHAR(100),
                    birth_date DATE,
                    group_id INTEGER
                )
            """)

            cur.execute("""
                CREATE TABLE Course_T5 (
                    course_id INTEGER,
                    title VARCHAR(100),
                    credits INTEGER,
                    start_date DATE
                )
            """)
            cur.execute("CREATE INDEX idx_t5_title_fts ON Course_T5 USING gin(to_tsvector('russian', title))")

            cur.execute("""
                CREATE TABLE Course_T6 (
                    course_id INTEGER,
                    title VARCHAR(100),
                    credits INTEGER,
                    start_date DATE
                )
            """)

            cur.close()
            print("Тестовые таблицы для исследования индексов созданы!")
            return True

    except Exception as e:
        print(f"Ошибка при создании тестовых таблиц: {e}")
        return False
