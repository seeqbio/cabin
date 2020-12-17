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

    def exists(self):
        with MYSQL.transaction() as cursor:
            cursor.execute("""
                SELECT COUNT(*)
                FROM system
                WHERE sha = '%s'
            """ % self.formula_sha)
            return cursor.fetchall()[0][0]

    def produce(self):
        with MYSQL.transaction() as cursor:
            self._create_table(cursor)
            self.import_table(cursor)
            self._update_system_table(cursor)

    @abstractmethod
    def import_table(self, cursor):
        pass

    @property
    def sql_drop_table(self):
        return 'DROP TABLE IF EXISTS `{table}`;'.format(table=self.table_name)

    @property
    def sql_drop_from_system(self):
        return 'DELETE FROM system WHERE name="{table}";'.format(table=self.table_name)


    def drop(self):
        with MYSQL.transaction() as cursor:
            cursor.execute(self.sql_drop_table)
            cursor.execute(self.sql_drop_from_system)

    def _create_table(self, cursor):
        query = self.schema.format(table=self.table_name).strip()
        cursor.create_table(self.table_name, query)

    def _update_system_table(self, cursor):
        query = ("""
            INSERT INTO system
            (type, name, formula, sha, table_name)
            VALUES
            ('%s', '%s', '%s', '%s', '%s');
        """ % (self.type, self.name, self.formula_json, self.formula_sha, self.table_name))
        cursor.execute(query)


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


class RecordByRecordImportMixin:
    from biodb import AbstractAttribute  # TODO: fix this
    columms = AbstractAttribute()
    """A list of columns as per SQL schema which is used to produce the
    `INSERT` command as well as to filter unwanted columns from the original
    source."""

    @property
    def sql_insert(self):
        return 'INSERT INTO `{table}` ({cols}) VALUES ({vals})'.format(
            table=self.table_name,
            cols=', '.join('`%s`' % col for col in self.columns),
            vals=', '.join('%({c})s'.format(c=col) for col in self.columns)
        )

    @abstractmethod
    def read(self):
        pass

    def import_table(self, cursor):
        for record in self.read():
            cursor.execute(self.sql_insert, record)
