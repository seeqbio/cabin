from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import LocalFile, ExternalFile
from biodb.io import read_csv

class pmkbOfficial(ExternalFile):
    version='therapies'

    @property
    def url(self):
        # version is always the same for drugs. Alternative urls exist but without downloadable data:
        # eg: https://pmkb.weill.cornell.edu/variants 
        return 'https://pmkb.weill.cornell.edu/{version}/downloadCSV.csv'.format(version=self.version)


class pmkbFile(LocalFile):
    version='1'
    depends= [pmkbOfficial]
    extension = 'csv'


class pmkbTable(RecordByRecordImportedTable):
    version='1'
    depends = [pmkbFile]

    columns = [
    'Gene',
    'Tumor_types',
    'Tissue_types',
    'Variants',
    'Tier'
    ]


    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                Gene          VARCHAR(255)    NOT NULL, -- gene symbol, not unique in dataset. eg: BRAF
                Tumor_types   VARCHAR(10055)  NOT NULL, -- '|' seperated, eg: Acute Myeloid Leukemia|T Lymphoblastic Leukemia/Lymphoma
                Tissue_types  VARCHAR(10255)  NOT NULL, -- '|' seperated, eg: Blood|Bone Marrow
                Variants      VARCHAR(1055)   NOT NULL, -- variant or consequences, written in words, eg: MPL codon(s) 515 missense|MPL W515L|MPL W515K
                Tier          INT             NOT NULL, -- 1: strongest evidence of clinical utility, 2: potential, 3: unknown
                INDEX(Gene)
            );
    """

    def read(self):
        for row in read_csv(self.inputs['pmkbFile'].path):
            row['Tumor_types'] = row.pop('Tumor Type(s)')
            row['Tissue_types'] = row.pop('Tissue Type(s)')
            row['Variants'] = row.pop('Variant(s)')
            yield row

