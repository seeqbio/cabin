import json
from abc import abstractmethod

from biodb import AbstractAttribute
from biodb import logger
from biodb import settings
from biodb.mysql import MYSQL
from biodb.data.core import Dataset, HistoricalDataset


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
                FROM `system`
                WHERE sha = '%s';
            """ % self.formula_sha)
            return cursor.fetchall()[0][0]

    def produce(self):
        with MYSQL.transaction(connection_kw={'allow_local_infile': True}) as cursor:
            self._create_table(cursor)
            self.import_table(cursor)
            self._update_system_table(cursor)
            logger.info("Imported %s rows to table: %s " % (self.get_nrows(cursor), self.table_name))

    @abstractmethod
    def import_table(self, cursor):
        pass

    @property
    def sql_drop_table(self):
        return 'DROP TABLE IF EXISTS `{table}`;'.format(table=self.table_name)

    @property
    def sql_drop_from_system(self):
        return 'DELETE FROM `system` WHERE name="{table}";'.format(table=self.table_name)

    def drop(self):
        with MYSQL.transaction() as cursor:
            cursor.execute(self.sql_drop_table)
            cursor.execute(self.sql_drop_from_system)

    def _create_table(self, cursor):
        query = self.schema.format(table=self.table_name).strip()
        cursor.create_table(self.table_name, query)

    def _update_system_table(self, cursor):
        instance_id = settings.SGX_INSTANCE_ID
        assert instance_id, 'Unset SGX_INSTANCE_ID'
        query = ("""
            INSERT INTO `system`
            (type, name, formula, sha, table_name, instance_id)
            VALUES
            ('%s', '%s', '%s', '%s', '%s', '%s');
        """ % (self.type, self.name, self.formula_json, self.formula_sha, self.table_name, instance_id))
        cursor.execute(query)

    def get_nrows(self, cursor):
        query = "SELECT COUNT(*) FROM `{table}`;".format(table=self.table_name)
        cursor.execute(query)
        result = cursor.fetchall()[0][0]
        return result

class RecordByRecordImportedTable(ImportedTable):
    columms = AbstractAttribute()
    """A list of columns as per SQL schema which is used to produce the
    `INSERT` command as well as to filter unwanted columns from the original
    source."""

    field_mappings = None
    """A dictionary of old column name (in original source) to new column name
    (as per SQL schema) which filters and renames the columns read from
    original source. By default no renaming or filtering is performed."""

    @property
    def sql_insert(self):
        return 'INSERT INTO `{table}` ({cols}) VALUES ({vals})'.format(
            table=self.table_name,
            cols=', '.join('`%s`' % col for col in self.columns),
            vals=', '.join('%({c})s'.format(c=col) for col in self.columns)
        )

    def import_table(self, cursor):
        for row in self.read():
            cursor.execute(self.sql_insert, self.transform(row))

    def transform(self, record):
        if self.field_mappings is None:
            return record
        else:
            return {
                new_col: record[old_col]
                for old_col, new_col in self.field_mappings.items()
            }


def imported_datasets(type=None):
    query = 'SELECT name, formula, sha FROM `system`;'
    if type:
        query += ' WHERE type = "%s"' % type

    with MYSQL.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

        for name, formula_json, sha in result:
            formula = json.loads(formula_json)
            yield HistoricalDataset(formula, name=name, sha=sha)
