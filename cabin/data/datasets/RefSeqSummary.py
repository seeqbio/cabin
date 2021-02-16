from biodb.io import read_xsv
from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import (
    FTPTimestampedFile,
    S3MirrorFile,
    S3MirroredLocalFile
)


class RefSeqSummaryOfficial(FTPTimestampedFile):
    version = '2020-08-18'  # Not updated daily

    ftp_server = 'hgdownload.soe.ucsc.edu'
    ftp_path = '/goldenPath/hgFixed/database/refSeqSummary.txt.gz'


class RefSeqSummaryS3Mirror(S3MirrorFile):
    version = '1'
    depends = [RefSeqSummaryOfficial]
    extension = 'gz'


class RefSeqSummaryFile(S3MirroredLocalFile):
    version = '1'
    depends = [RefSeqSummaryS3Mirror]
    extension = 'gz'


class RefSeqSummaryTable(RecordByRecordImportedTable):
    version = '1'
    depends = [RefSeqSummaryFile]
    tags = ['active']

    columns = ['refseq_transcript', 'summary']

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                refseq_transcript VARCHAR(255) NOT NULL UNIQUE,
                summary           TEXT NOT NULL
            )
        """

    def read(self):
        columns = ['refseq_transcript', 'completeness', 'summary']
        for row in read_xsv(self.input.path, columns=columns, gzipped=True):
            if 'summary' not in row:
                continue
            yield row
