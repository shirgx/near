import os
import re
from storage import TableSchema, Column, DataType, BinaryStorage
from index import NumberIndex

class SimpleDB:
    def __init__(self, db_path="db_files"):
        self.db_path = db_path
        if not os.path.exists(db_path):
            os.makedirs(db_path)
        self.tables = {}
        self.indexes = {}

    def execute_sql(self, sql):
        sql = sql.strip()

        if sql.upper().startswith("CREATE TABLE"):
            return self._create_table(sql)
        elif sql.upper().startswith("SELECT"):
            return self._select(sql)
        elif sql.upper().startswith("INSERT"):
            return self._insert(sql)
        elif sql.upper().startswith("DELETE"):
            return self._delete(sql)
        elif sql.upper().startswith("CREATE INDEX"):
            return self._create_index(sql)
        else:
            raise Exception("Неподдерживаемый SQL запрос")

    def _create_table(self, sql):
        match = re.match(r'CREATE TABLE (\w+) \((.*)\)', sql, re.IGNORECASE)
        if not match:
            raise Exception("Неправильный синтаксис CREATE TABLE")

        table_name = match.group(1)
        cols_str = match.group(2)

        columns = []
        for col_def in cols_str.split(','):
            col_def = col_def.strip()
            if 'INT' in col_def.upper():
                col_name = col_def.split()[0]
                columns.append(Column(col_name, DataType.INT))
            elif 'VARCHAR' in col_def.upper():
                match_varchar = re.match(r'(\w+)\s+VARCHAR\((\d+)\)', col_def, re.IGNORECASE)
                if match_varchar:
                    col_name = match_varchar.group(1)
                    size = int(match_varchar.group(2))
                    columns.append(Column(col_name, DataType.VARCHAR, size))

        schema = TableSchema(table_name, columns)
        schema_file = os.path.join(self.db_path, f"{table_name}.schema")
        data_file = os.path.join(self.db_path, f"{table_name}.data")

        schema.save_schema(schema_file)
        self.tables[table_name] = BinaryStorage(data_file, schema)

        return f"Таблица {table_name} создана"

    def _load_table(self, table_name):
        if table_name not in self.tables:
            schema_file = os.path.join(self.db_path, f"{table_name}.schema")
            data_file = os.path.join(self.db_path, f"{table_name}.data")

            if not os.path.exists(schema_file):
                raise Exception(f"Таблица {table_name} не существует")

            schema = TableSchema.load_schema(schema_file)
            self.tables[table_name] = BinaryStorage(data_file, schema)

    def _select(self, sql):
        from_match = re.search(r'FROM (\w+)', sql, re.IGNORECASE)
        if not from_match:
            raise Exception("Неправильный синтаксис SELECT")

        table_name = from_match.group(1)
        self._load_table(table_name)
        storage = self.tables[table_name]

        where_match = re.search(r'WHERE (\w+) = (.+)', sql, re.IGNORECASE)

        if where_match:
            col_name = where_match.group(1)
            value_str = where_match.group(2).strip().strip('"\'')

            col_index = None
            col_type = None
            for i, col in enumerate(storage.schema.columns):
                if col.name == col_name:
                    col_index = i
                    col_type = col.data_type
                    break

            if col_index is None:
                raise Exception(f"Столбец {col_name} не найден")

            if col_type == DataType.INT:
                value = int(value_str)
            else:
                value = value_str

            index_key = f"{table_name}_{col_name}"
            if index_key in self.indexes and col_type == DataType.INT:
                row_indices = self.indexes[index_key].find_rows(value)
                rows = []
                for row_idx in row_indices:
                    row = storage.get_row(row_idx)
                    if row:
                        rows.append(row)
            else:
                all_rows = storage.get_all_rows()
                rows = [row for row in all_rows if row[col_index] == value]
        else:
            rows = storage.get_all_rows()

        select_match = re.match(r'SELECT (.+?) FROM', sql, re.IGNORECASE)
        if select_match:
            cols_str = select_match.group(1).strip()
            if cols_str == '*':
                return rows
            else:
                col_names = [name.strip() for name in cols_str.split(',')]
                col_indices = []
                for col_name in col_names:
                    for i, col in enumerate(storage.schema.columns):
                        if col.name == col_name:
                            col_indices.append(i)
                            break

                return [[row[i] for i in col_indices] for row in rows]

        return rows

    def _insert(self, sql):
        match = re.match(r'INSERT INTO (\w+) VALUES \((.+)\)', sql, re.IGNORECASE)
        if not match:
            raise Exception("Неправильный синтаксис INSERT")

        table_name = match.group(1)
        values_str = match.group(2)

        self._load_table(table_name)
        storage = self.tables[table_name]

        values = []
        for i, val_str in enumerate(values_str.split(',')):
            val_str = val_str.strip().strip('"\'')
            col = storage.schema.columns[i]

            if col.data_type == DataType.INT:
                values.append(int(val_str))
            else:
                values.append(val_str)

        row_index = storage.get_row_count()
        storage.insert_row(values)

        for i, col in enumerate(storage.schema.columns):
            index_key = f"{table_name}_{col.name}"
            if index_key in self.indexes and col.data_type == DataType.INT:
                self.indexes[index_key].add_entry(values[i], row_index)

        return "Строка добавлена"

    def _delete(self, sql):
        if "DELETE *" in sql.upper():
            match = re.search(r'FROM (\w+)', sql, re.IGNORECASE)
            if not match:
                raise Exception("Неправильный синтаксис DELETE")

            table_name = match.group(1)
            self._load_table(table_name)
            storage = self.tables[table_name]

            storage.delete_all_rows()

            for col in storage.schema.columns:
                index_key = f"{table_name}_{col.name}"
                if index_key in self.indexes:
                    self.indexes[index_key].clear_index()

            return "Все данные удалены"
        else:
            where_match = re.search(r'WHERE (\w+) = (.+)', sql, re.IGNORECASE)
            if not where_match:
                raise Exception("Неправильный синтаксис DELETE")

            table_match = re.search(r'FROM (\w+)', sql, re.IGNORECASE)
            if not table_match:
                raise Exception("Неправильный синтаксис DELETE")

            table_name = table_match.group(1)
            col_name = where_match.group(1)
            value_str = where_match.group(2).strip().strip('"\'')

            self._load_table(table_name)
            storage = self.tables[table_name]

            col_type = None
            for col in storage.schema.columns:
                if col.name == col_name:
                    col_type = col.data_type
                    break

            if col_type == DataType.INT:
                value = int(value_str)
            else:
                value = value_str

            storage.delete_rows_by_condition(col_name, value)

            for col in storage.schema.columns:
                index_key = f"{table_name}_{col.name}"
                if index_key in self.indexes:
                    self.indexes[index_key].rebuild_index(storage)

            return f"Строки с {col_name} = {value} удалены"

    def _create_index(self, sql):
        match = re.match(r'CREATE INDEX ON (\w+) \((\w+)\)', sql, re.IGNORECASE)
        if not match:
            raise Exception("Неправильный синтаксис CREATE INDEX")

        table_name = match.group(1)
        col_name = match.group(2)

        self._load_table(table_name)
        storage = self.tables[table_name]

        col_found = False
        for col in storage.schema.columns:
            if col.name == col_name and col.data_type == DataType.INT:
                col_found = True
                break

        if not col_found:
            raise Exception(f"Столбец {col_name} не найден или не является INT")

        index_key = f"{table_name}_{col_name}"
        index_file = os.path.join(self.db_path, f"{index_key}.index")

        index = NumberIndex(index_file, col_name)
        index.rebuild_index(storage)
        self.indexes[index_key] = index

        return f"Индекс на {table_name}.{col_name} создан"
