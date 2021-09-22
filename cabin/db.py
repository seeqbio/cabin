import json
from abc import abstractmethod

from . import logger, settings, AbstractAttribute
from .mysql import MYSQL, WRITER
from .core import Dataset, HistoricalDataset


class ImportedTable(Dataset):
    @property
    @abstractmethod
    def schema(self):
        pass

    @property
    def table_name(self):
        return self.name

    @property
    def input_table_names(self):
        return {type: input.table_name for type, input in self.inputs.items()}

    def exists(self):
        with MYSQL.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*)
                FROM `system`
                WHERE sha = '%s';
            """ % self.formula_sha)
            return cursor.fetchall()[0][0]

    def produce(self):
        try:
            with MYSQL.cursor(user=WRITER) as cursor:
                self._create_table(cursor)
                self.import_table(cursor)
                self._update_system_table(cursor)
                logger.info("Imported %s rows to table: %s " % (self.get_nrows(cursor), self.table_name))
        except BaseException as e:
            # catch everything to cleanup, even KeyboardInterrupt.
            #
            # Note: if the user hits ctrl+c twice in a row, then we may not
            # finish actually cleaning up because the second KeyboardInterrupt
            # will interupt the clean up logic.
            #
            # use a different cursor for cleanup, the original one is possibly
            # mid-read/write and we'd get a packet out of order error.
            with MYSQL.cursor(user=WRITER) as cursor:
                logger.info('Import failed, cleaning up before exiting. This may take some time...')
                self.failed_import_cleanup(cursor)

            raise e

    @abstractmethod
    def import_table(self, cursor):
        pass

    def failed_import_cleanup(self, cursor):
        cursor.execute(self.sql_drop_table)
        cursor.execute(self.sql_drop_from_system)

    @property
    def sql_drop_table(self):
        return 'DROP TABLE IF EXISTS `{table}`;'.format(table=self.table_name)

    @property
    def sql_drop_from_system(self):
        return 'DELETE FROM `system` WHERE name="{table}";'.format(table=self.table_name)

    def _create_table(self, cursor):
        query = self.schema.format(table=self.table_name).strip()
        cursor.execute(query)

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


def imported_tables(latest_only=False, type=None):
    query = 'SELECT name, formula, sha FROM `system`'
    if type:
        query += ' WHERE type = "%s"' % type
    query += ' ORDER BY name'

    with MYSQL.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

        for name, formula_json, sha in result:
            formula = json.loads(formula_json)
            hd = HistoricalDataset(formula, name=name, sha=sha)
            if not latest_only or hd.is_latest():
                yield hd
