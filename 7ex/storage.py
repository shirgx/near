import struct
import os
import json

class DataType:
    INT = 1
    VARCHAR = 2

class Column:
    def __init__(self, name, data_type, size=None):
        self.name = name
        self.data_type = data_type
        self.size = size

    def to_dict(self):
        return {
            'name': self.name,
            'data_type': self.data_type,
            'size': self.size
        }

    @classmethod
    def from_dict(cls, data):
        return cls(data['name'], data['data_type'], data['size'])

class TableSchema:
    def __init__(self, table_name, columns):
        self.table_name = table_name
        self.columns = columns
        self.row_size = self._calculate_row_size()

    def _calculate_row_size(self):
        size = 0
        for col in self.columns:
            if col.data_type == DataType.INT:
                size += 8
            elif col.data_type == DataType.VARCHAR:
                size += col.size
        return size

    def save_schema(self, schema_file):
        schema_data = {
            'table_name': self.table_name,
            'columns': [col.to_dict() for col in self.columns],
            'row_size': self.row_size
        }
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f)

    @classmethod
    def load_schema(cls, schema_file):
        with open(schema_file, 'r') as f:
            schema_data = json.load(f)

        columns = [Column.from_dict(col_data) for col_data in schema_data['columns']]
        return cls(schema_data['table_name'], columns)

class BinaryStorage:
    def __init__(self, data_file, schema):
        self.data_file = data_file
        self.schema = schema

    def pack_row(self, values):
        packed_data = b''

        for i, col in enumerate(self.schema.columns):
            if i >= len(values):
                value = None
            else:
                value = values[i]

            if col.data_type == DataType.INT:
                if value is None:
                    packed_data += struct.pack('<Q', 0)
                else:
                    packed_data += struct.pack('<Q', int(value))

            elif col.data_type == DataType.VARCHAR:
                if value is None:
                    value = ""

                value_str = str(value)
                if len(value_str) > col.size:
                    value_str = value_str[:col.size]

                value_bytes = value_str.encode('utf-8')
                if len(value_bytes) > col.size:
                    value_bytes = value_bytes[:col.size]

                padded_value = value_bytes.ljust(col.size, b'\x00')
                packed_data += padded_value

        return packed_data

    def unpack_row(self, packed_data):
        values = []
        offset = 0

        for col in self.schema.columns:
            if col.data_type == DataType.INT:
                value = struct.unpack('<Q', packed_data[offset:offset+8])[0]
                if value == 0:
                    values.append(None)
                else:
                    values.append(value)
                offset += 8

            elif col.data_type == DataType.VARCHAR:
                value_bytes = packed_data[offset:offset+col.size]
                value_str = value_bytes.rstrip(b'\x00').decode('utf-8', errors='ignore')
                values.append(value_str if value_str else None)
                offset += col.size

        return values

    def insert_row(self, values):
        packed_row = self.pack_row(values)

        with open(self.data_file, 'ab') as f:
            f.write(packed_row)

    def get_row_count(self):
        if not os.path.exists(self.data_file):
            return 0

        file_size = os.path.getsize(self.data_file)
        return file_size // self.schema.row_size

    def get_row(self, row_index):
        if not os.path.exists(self.data_file):
            return None

        with open(self.data_file, 'rb') as f:
            f.seek(row_index * self.schema.row_size)
            packed_data = f.read(self.schema.row_size)

            if len(packed_data) < self.schema.row_size:
                return None

            return self.unpack_row(packed_data)

    def get_all_rows(self):
        if not os.path.exists(self.data_file):
            return []

        rows = []
        row_count = self.get_row_count()

        for i in range(row_count):
            row = self.get_row(i)
            if row is not None:
                rows.append(row)

        return rows

    def delete_all_rows(self):
        if os.path.exists(self.data_file):
            os.remove(self.data_file)

    def delete_rows_by_condition(self, column_name, value):
        if not os.path.exists(self.data_file):
            return 0

        col_index = None
        for i, col in enumerate(self.schema.columns):
            if col.name == column_name:
                col_index = i
                break

        if col_index is None:
            return 0

        all_rows = self.get_all_rows()
        self.delete_all_rows()

        deleted_count = 0
        for row in all_rows:
            if len(row) > col_index and row[col_index] == value:
                deleted_count += 1
            else:
                self.insert_row(row)

        return deleted_count
