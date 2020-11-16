import json
import sqlite3
from pathlib import Path
from abc import abstractmethod

from .core import Dataset, HistoricalDataset

DB_PATH = 'mock_storage/db.sqlite3'


class ImportedTable(Dataset):
    @property
    @abstractmethod
    def schema(self):
        pass

    @property
    def table_name(self):
        return self.name

    def create_table(self):
        query = self.schema.format(table=self.table_name).strip()
        print('%s\n' % query)

        execute_sql("""
            INSERT INTO system
            (type, name, formula, sha, table_name)
            VALUES
            ('%s', '%s', '%s', '%s', '%s')
        """ % (self.type, self.name, self.formula_json, self.formula_sha, self.table_name))

    def exists(self):
        return bool(execute_sql("""
            SELECT *
            FROM system
            WHERE sha = '%s'
        """ % self.formula_sha))


# decorator for produce() of imported tables
def atomic_transaction(**connection_kw):
    # mock
    def decorator(func):
        def wrapped(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapped
    return decorator


def execute_sql(query):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(query)

    res = [r for r in cursor]

    cursor.close()
    conn.commit()
    conn.close()

    return res


def init_db():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

    query = """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        AND name='system';
    """
    if execute_sql(query):
        return

    print('--> initializing database')
    execute_sql("""
        CREATE TABLE system (
            sha         VARCHAR(64) PRIMARY KEY,
            type        VARCHAR(128),
            name       VARCHAR(255),
            table_name  VARCHAR(255),
            formula     VARCHAR(4096)
        );
    """)


def imported_datasets(type=None):
    query = 'SELECT name, formula, sha FROM system'
    if type:
        query += ' WHERE type = "%s"' % type
    for name, formula_json, sha in execute_sql(query):
        formula = json.loads(formula_json)
        yield HistoricalDataset(formula, name=name, sha=sha)
