import json
import os

class NumberIndex:
    def __init__(self, index_file, column_name):
        self.index_file = index_file
        self.column_name = column_name
        self.index_data = {}

    def add_entry(self, value, row_index):
        if value not in self.index_data:
            self.index_data[value] = []
        self.index_data[value].append(row_index)

    def find_rows(self, value):
        return self.index_data.get(value, [])

    def rebuild_index(self, storage):
        self.index_data = {}
        rows = storage.get_all_rows()

        col_index = None
        for i, col in enumerate(storage.schema.columns):
            if col.name == self.column_name:
                col_index = i
                break

        if col_index is None:
            return

        for row_index, row in enumerate(rows):
            value = row[col_index]
            self.add_entry(value, row_index)

    def clear_index(self):
        self.index_data = {}
        if os.path.exists(self.index_file):
            os.remove(self.index_file)
