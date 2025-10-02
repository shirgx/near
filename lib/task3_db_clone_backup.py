import psycopg2
import csv
import os
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

@contextmanager
def admin_db_connection(host, user, password):
    conn = None
    try:
        conn = psycopg2.connect(
            host=host,
            dbname="postgres",
            user=user,
            password=password
        )
        conn.autocommit = True
        yield conn
    except Exception as e:
        raise e
    finally:
        if conn:
            conn.close()

def create_database(dbname, host, user, password):
    try:
        with admin_db_connection(host, user, password) as conn:
            cur = conn.cursor()
            cur.execute(f'CREATE DATABASE "{dbname}"')
            cur.close()
        return True
    except Exception as e:
        print(f"Ошибка создания базы данных: {e}")
        return False

def drop_database(dbname, host, user, password):
    try:
        with admin_db_connection(host, user, password) as conn:
            cur = conn.cursor()
            cur.execute(f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity 
                WHERE datname = '{dbname}' AND pid <> pg_backend_pid()
            """)
            cur.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
            cur.close()
        return True
    except Exception as e:
        print(f"Ошибка удаления базы данных: {e}")
        return False

def backup_table_to_csv(table_name, file_path, host, dbname, user, password):
    try:
        with db_connection(host, dbname, user, password) as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM {table_name}")
            rows = cur.fetchall()

            cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s 
                ORDER BY ordinal_position
            """, (table_name.lower(),))
            columns = [row[0] for row in cur.fetchall()]

            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(columns)
                writer.writerows(rows)

            cur.close()
        return True
    except Exception as e:
        print(f"Ошибка создания бэкапа таблицы {table_name}: {e}")
        return False

def restore_table_from_csv(table_name, file_path, host, dbname, user, password):
    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            columns = next(reader)

            with db_connection(host, dbname, user, password) as conn:
                cur = conn.cursor()
                cur.execute(f"DELETE FROM {table_name}")

                placeholders = ', '.join(['%s'] * len(columns))
                query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

                for row in reader:
                    processed_row = []
                    for value in row:
                        if value == '':
                            processed_row.append(None)
                        else:
                            processed_row.append(value)
                    cur.execute(query, processed_row)

                cur.close()
        return True
    except Exception as e:
        print(f"Ошибка восстановления таблицы {table_name}: {e}")
        return False

class DatabaseManager:
    def __init__(self, host, dbname, user, password):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password

    def create_sandbox(self, sandbox_name):
        # Сначала удаляем базу если она существует, затем создаем новую
        drop_database(sandbox_name, self.host, self.user, self.password)
        return create_database(sandbox_name, self.host, self.user, self.password)

    def drop_sandbox(self, sandbox_name):
        return drop_database(sandbox_name, self.host, self.user, self.password)

    def clone_schema_to_sandbox(self, sandbox_name):
        try:
            with db_connection(self.host, self.dbname, self.user, self.password) as source_conn:
                source_cur = source_conn.cursor()

                source_cur.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                """)
                tables = [row[0] for row in source_cur.fetchall()]

                with db_connection(self.host, sandbox_name, self.user, self.password) as target_conn:
                    target_cur = target_conn.cursor()

                    for table in tables:
                        source_cur.execute(f"""
                            SELECT column_name, data_type, character_maximum_length,
                                   is_nullable, column_default
                            FROM information_schema.columns 
                            WHERE table_name = %s 
                            ORDER BY ordinal_position
                        """, (table,))

                        columns_info = source_cur.fetchall()

                        create_sql = f"CREATE TABLE {table} ("
                        column_definitions = []

                        for col_name, data_type, max_length, is_nullable, col_default in columns_info:
                            col_def = f"{col_name} "

                            if data_type == 'character varying' and max_length:
                                col_def += f"VARCHAR({max_length})"
                            elif data_type == 'integer':
                                col_def += "INTEGER"
                            elif data_type == 'date':
                                col_def += "DATE"
                            else:
                                col_def += data_type.upper()

                            if is_nullable == 'NO':
                                col_def += " NOT NULL"

                            if col_default and 'nextval' in col_default:
                                if 'SERIAL' not in col_def:
                                    col_def = col_name + " SERIAL"
                                    if is_nullable == 'NO':
                                        col_def += " NOT NULL"

                            column_definitions.append(col_def)

                        create_sql += ", ".join(column_definitions) + ")"
                        target_cur.execute(create_sql)

                    target_cur.close()
                source_cur.close()
            return True
        except Exception as e:
            print(f"Ошибка клонирования схемы: {e}")
            return False

    def backup_all_tables(self, backup_dir):
        os.makedirs(backup_dir, exist_ok=True)
        tables = ["GroupTable", "Course", "Student", "StudentCard", "Enrollment"]

        for table in tables:
            file_path = os.path.join(backup_dir, f"{table}.csv")
            if not backup_table_to_csv(table, file_path, self.host, self.dbname, self.user, self.password):
                return False
        return True

    def restore_all_tables(self, backup_dir):
        tables_order = ["GroupTable", "Course", "Student", "StudentCard", "Enrollment"]

        try:
            with db_connection(self.host, self.dbname, self.user, self.password) as conn:
                cur = conn.cursor()

                for table in reversed(tables_order):
                    cur.execute(f"DELETE FROM {table}")

                for table in tables_order:
                    file_path = os.path.join(backup_dir, f"{table}.csv")
                    if os.path.exists(file_path):
                        with open(file_path, 'r', encoding='utf-8') as csvfile:
                            reader = csv.reader(csvfile)
                            columns = next(reader)

                            placeholders = ', '.join(['%s'] * len(columns))
                            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

                            for row in reader:
                                processed_row = []
                                for value in row:
                                    if value == '':
                                        processed_row.append(None)
                                    else:
                                        processed_row.append(value)
                                cur.execute(query, processed_row)

                cur.close()
            return True
        except Exception as e:
            print(f"Ошибка восстановления данных: {e}")
            return False
