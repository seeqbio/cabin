from biodb.io import read_xsv
from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import (
    FTPTimestampedFile,
    S3MirrorFile,
    S3MirroredLocalFile
)


class refseq_summaryOfficial(FTPTimestampedFile):
    version = '2020-08-18'  # Not updated daily

    ftp_server = 'hgdownload.soe.ucsc.edu'
    ftp_path = '/goldenPath/hgFixed/database/refSeqSummary.txt.gz'


class refseq_summaryS3Mirror(S3MirrorFile):
    version = '1'
    depends = [refseq_summaryOfficial]
    extension = 'gz'


class refseq_summaryFile(S3MirroredLocalFile):
    version = '1'
    depends = [refseq_summaryS3Mirror]
    extension = 'gz'


class refseq_summaryTable(RecordByRecordImportedTable):
    version = '1'
    depends = [refseq_summaryFile]

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