from abc import abstractmethod
from biodb import BiodbError
from biodb import AbstractAttribute
from biodb.io import ftp_modify_time


class FtpTimestampedMixin:
    ftp_server = AbstractAttribute()
    ftp_path = AbstractAttribute()

    @property
    def source_url(self):
        timestamp = ftp_modify_time(self.ftp_server, self.ftp_path)
        # version = 'v' + str(timestamp.date())
        version = str(timestamp.date())
        if version != self.version.source:
            raise BiodbError('Cannot download {label}, available version: "{v}"'.format(label=self.label, v=version))
        return 'ftp://{s}{p}'.format(s=self.ftp_server, p=self.ftp_path)


class RecordByRecordImportMixin:
    field_mappings = None # dictionary of old to new (filter and rename), if None no filtering or renaming
    columns = AbstractAttribute()

    @property
    def sql_insert(self):
        return 'INSERT INTO `{table}` ({cols}) VALUES ({vals})'.format(
            table=self.table_name,
            cols=', '.join('`%s`' % col for col in self.columns),
            vals=', '.join('%({c})s'.format(c=col) for col in self.columns)
        )

    def transform(self, record):
        if self.field_mappings is None:
            return record
        else:
            return {new_col: record[old_col] for old_col, new_col in self.field_mappings.items()}

    @abstractmethod
    def read(self):
        # yields dictionaries exactly matching sql schema
        pass

    def import_real(self):
        with self.app.mysql.transaction() as cursor:
            cursor.create_table(self.table_name, self.sql_create)
            # execute is ever so slightly slower than executemany, but
            # executemany requires us to manually buffer our records (does not
            # accept a generator).
            for record in self.read():
                cursor.execute(self.sql_insert, self.transform(record))
