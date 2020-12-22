from biodb.data.db import ImportedTable
from biodb.data.files import LocalFile, ExternalFile


class dgiOfficial(ExternalFile):
    version='2020-Nov'

    @property
    def url(self):
        return 'https://dgidb.org/data/monthly_tsvs/{version}/interactions.tsv'.format(version=self.version)


class dgiFile(LocalFile):
    version='1'
    depends= [dgiOfficial]
    extension = 'tsv'


class dgiTable(ImportedTable): 
    version='1'
    depends = [dgiFile]

    @property
    def schema(self):
    	return """
            CREATE TABLE `{table}` (
                gene_name                  VARCHAR(255),        -- entrez gene name
                gene_claim_name            VARCHAR(255),        -- ?
                entrez_id                  VARCHAR(255),
                interaction_claim_source   VARCHAR(255),        -- eg: CancerCommons, DTC, JAX-CKB, NCI, GuideToPharmacology
                interaction_types          VARCHAR(255),        -- eg: inhibitor, blocker, agonist, allosteric modulator.. see https://dgidb.org/interaction_types for docs
                drug_claim_name            VARCHAR(255),        -- ? 
                drug_claim_primary_name    VARCHAR(255),        -- ? 
                drug_name                  VARCHAR(255),        -- UPPER, eg: OXAZEPAM, CHEMBL528694, SB-202190
                drug_concept_id            VARCHAR(255),        -- seem like dgi standard rather than source-specific. eg: chembl:CHEMBL1502, chembl:CHEMBL515
                PMIDs                      VARCHAR(255),
                INDEX (entrez_id)
            );
        """
