import json
import sqlite3
from pathlib import Path
from abc import abstractmethod
from biodb.mysql import MYSQL
from .core import Dataset, HistoricalDataset
 

class ImportedTable(Dataset):
    @property
    @abstractmethod
    def schema(self):
        pass

    @property
    def table_name(self):
        return self.name

    def create_table(self):
        with MYSQL.transaction() as cursor:
            # Create empty table
            query = self.schema.format(table=self.table_name).strip()
            cursor.execute(query) ## TODO: made this into cursor.create_table()

            # instert table info into system
            query = ("""
                INSERT INTO system
                (type, name, formula, sha, table_name)
                VALUES
                ('%s', '%s', '%s', '%s', '%s');
            """ % (self.type, self.name, self.formula_json, self.formula_sha, self.table_name))
            cursor.execute(query) 


    def exists(self):
        with MSQL.transcation() as cursor:
            return bool(cursor.execute("""
                SELECT *
                FROM system
                WHERE sha = '%s'
            """ % self.formula_sha))


def execute_sql(query):
    with MYSQL.transaction() as cursor:
        res = cursor.execute(query)
        return res


def imported_datasets(type=None):
    query = 'SELECT name, formula, sha FROM system'
    if type:
        query += ' WHERE type = "%s"' % type
    for name, formula_json, sha in execute_sql(query):
        formula = json.loads(formula_json)
        yield HistoricalDataset(formula, name=name, sha=sha)
