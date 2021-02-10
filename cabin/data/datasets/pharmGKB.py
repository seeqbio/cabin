from biodb.data.db import ImportedTable
from biodb.data.files import LocalFile, ExternalFile

from biodb import settings
from biodb.io import unzip


class pharmGKBOfficial(ExternalFile):
    version = 'v1'

    @property
    def url(self):
        return 'https://api.pharmgkb.org/{version}/download/file/data/relationships.zip'.format(version=self.version)


class pharmGKBFile(LocalFile):
    version = '1'
    depends = [pharmGKBOfficial]
    extension = 'zip'

    @property
    def path(self):
        # FIXME this is not ok because:
        # 1. tries to unzip on every access to .path
        # 2. tries to unzip a non-existent file in --dry-run
        expanded_dir = unzip('{downloads}/{name}.{ext}'.format(
            downloads=settings.SGX_DOWNLOAD_DIR,
            name=self.name,
            ext=self.extension
        ))
        return '{downloads}/{expanded_dir_name}/relationships.tsv'.format(
            downloads=settings.SGX_DOWNLOAD_DIR,
            expanded_dir_name=expanded_dir.name
        )


class pharmGKBTable(ImportedTable):
    version = '1'
    depends = [pharmGKBFile]

    @property
    def schema(self):
    	return """
          CREATE TABLE `{table}` (
            Entity1_id	               VARCHAR(225),         -- Entities are disease, gene, drugs by PharmGKB ID. mappings to names are in seperate files
            Entity1_name               VARCHAR(225),         -- Eg: Carcinoma, imatinib, BRAF, rs119774 or CYP3A4 14200T>G, c.1777G>A or CYP2D6*4
            Entity1_type               VARCHAR(225),         -- Disease, Chemical, Gene, Variant (rsid or chromosomal position) or Haplotype (eg: CYP2D6*4))
            Entity2_id	               VARCHAR(225),
            Entity2_name	       VARCHAR(225),
            Entity2_type	       VARCHAR(225),
            Evidence	               VARCHAR(225),
            Association	               VARCHAR(225),
            PK	                       VARCHAR(225),         -- relationships marked with PK if entities found in pharmacokinetic pathway on pharmGKB or other explicit phamacokinetic interaction
            PD	                       VARCHAR(225),         -- relationships marked with PD if entities ofund in pharmacodynamic pathway on pharmGKB or other explicity pharmacodynamic interaction
            PMIDs                      VARCHAR(225)
        );
        """


    def import_table(self, cursor):
        cursor.execute("""
            LOAD DATA LOCAL INFILE '{path}'
            INTO TABLE `{table}`
        """.format(path=self.input.path, table=self.table_name)) # r"""
