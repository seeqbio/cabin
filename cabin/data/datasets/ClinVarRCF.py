import gzip

from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import LocalFile, ExternalFile

from biodb.io import read_xml
from biodb.io import xml_element_clear_memory
from biodb.io import get_xrefs_by_db


class ClinVarRCVOfficial(ExternalFile):
    version = '2020-12'

    @property
    def url(self):
        return 'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/ClinVarFullRelease_{version}.xml.gz'.format(version=self.version)


class ClinVarRCVFile(LocalFile):
    version = '1'
    depends = [ClinVarRCVOfficial]
    extension = 'xml.gz'


class ClinVarRCVTable(RecordByRecordImportedTable):
    version = ' 1'
    depends = [ClinVarRCVFile]

    columns = [
        'rcv_accession',
        'rcv_clinsig',
        'variation_id',
        'evidence_pmids',
        'rcv_traits',
        'mesh_ids',
        'medgen_ids',
        'omim_ids',
        'hpo_ids',
        'mondo_ids'
    ]

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                rcv_accession              VARCHAR(255)   PRIMARY KEY,
                rcv_clinsig                VARCHAR(1023)  NOT NULL,
                variation_id               VARCHAR(255)   NOT NULL,
                evidence_pmids             VARCHAR(5844)  NULL,
                rcv_traits                 LONGTEXT       NOT NULL,
                mesh_ids                   VARCHAR(255)   NOT NULL,  -- eg: MESH:D008375|MESH:D008376
                medgen_ids                 VARCHAR(2555)  NOT NULL,  -- eg: MedGen:C0024776|MedGen:CN51720
                omim_ids                   VARCHAR(255)   NOT NULL,  -- eg: OMIM:248600|OMIM:PS248600
                hpo_ids                    VARCHAR(1255)  NOT NULL,  -- eg: HP:0010862|HP:0010864
                mondo_ids                  VARCHAR(955)   NOT NULL,  -- eg: MONDO:0015280|MONDO:0018997
                INDEX (variation_id)
            );
        """

    def read(self):
        with gzip.open(str(self.input.path), 'rb') as source:
            for elem in read_xml(source, tag='ClinVarSet'):
                rcv = elem.xpath('./ReferenceClinVarAssertion')[0]
                rcv_accession = rcv.xpath('./ClinVarAccession')[0].get('Acc')
                rcv_clinsig = rcv.xpath('./ClinicalSignificance/Description')[0].text

                # extract all xrefs for disease dbs:
                xref_by_db = get_xrefs_by_db(rcv.xpath('./TraitSet/Trait'))
                rcv_traits = [elem.text for elem in rcv.xpath('./TraitSet/Trait/Name/ElementValue')]
                try:
                    variation_id = rcv.xpath('./MeasureSet')[0].get('ID')
                except IndexError:
                    continue
                finally:
                    xml_element_clear_memory(rcv)

                try:
                    evidence_pmids = set(e.text for e in rcv.xpath('./ObservedIn/ObservedData/Citation/ID[@Source="PubMed"]'))
                except IndexError:
                    evidence_pmids = None

                rec = {
                    'rcv_accession': rcv_accession,
                    'rcv_clinsig': rcv_clinsig,
                    'variation_id': variation_id,
                    'evidence_pmids': '|'.join(evidence_pmids),
                    'rcv_traits': '|'.join(rcv_traits),
                    'mesh_ids': '|'.join(xref_by_db['MeSH']),
                    'medgen_ids': '|'.join(xref_by_db['MedGen']),
                    'omim_ids': '|'.join(xref_by_db['OMIM']),
                    'hpo_ids': '|'.join(xref_by_db['Human Phenotype Ontology']),
                    'mondo_ids': '|'.join(xref_by_db['MONDO'])
                }
                yield rec
                xml_element_clear_memory(elem)
