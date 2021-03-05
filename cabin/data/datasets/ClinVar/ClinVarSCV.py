import gzip

from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import LocalFile, ExternalFile
from biodb.data.datasets.ClinVar.common import get_xrefs_by_db
from biodb.io import read_xml
from biodb.io import xml_element_clear_memory


class ClinVarSCVOfficial(ExternalFile):
    version = '2020-06'

    @property
    def url(self):
        return 'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/ClinVarFullRelease_{version}.xml.gz'.format(version=self.version)


class ClinVarSCVFile(LocalFile):
    version = '1'
    depends = [ClinVarSCVOfficial]
    extension = 'xlm.gz'


class ClinVarSCVTable(RecordByRecordImportedTable):
    version = '2'
    depends = [ClinVarSCVFile]
    tags = ['active']

    columns = [
        'variation_id',
        'rcv_accession',
        'scv_accession',
        'scv_clinsig',
        'scv_traits',
        'evidence_pmids',
        'evidence_descriptions',
        'mesh_ids',
        'medgen_ids',
        'omim_ids',
        'hpo_ids',
    ]

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                scv_accession              VARCHAR(255)   PRIMARY KEY,
                variation_id               VARCHAR(255)   NOT NULL,
                rcv_accession              VARCHAR(255)   NOT NULL,
                scv_clinsig                VARCHAR(255)   NULL,
                scv_traits                 LONGTEXT       NOT NULL,
                evidence_pmids             VARCHAR(5844)  NOT NULL,
                evidence_descriptions      LONGTEXT       NOT NULL,  -- description of observations / sumamry of literature findings
                mesh_ids                   VARCHAR(255)   NOT NULL,  -- eg: MESH:D008375|MESH:D008376
                medgen_ids                 VARCHAR(2555)  NOT NULL,  -- eg: MedGen:C0024776|MedGen:CN51720
                omim_ids                   VARCHAR(255)   NOT NULL,  -- eg: OMIM:248600|OMIM:PS248600
                hpo_ids                    VARCHAR(1255)  NOT NULL,  -- eg: HP:0010862|HP:0010864
                INDEX (variation_id)
            );
    """

    def read(self):
        with gzip.open(str(self.input.path), 'rb') as source:
            for elem in read_xml(source, tag='ClinVarSet'):
                rcv_accession_list = elem.xpath('./ReferenceClinVarAssertion/ClinVarAccession')
                assert len(rcv_accession_list) == 1, "Cannot have more than one RCV per ClinVarSet"
                rcv_accession = rcv_accession_list[0].get('Acc')

                try:
                    variation_id = elem.xpath('./ReferenceClinVarAssertion/MeasureSet')[0].get('ID')
                except IndexError:
                    continue  # drop variants with no variant id

                for scv in elem.xpath('./ClinVarAssertion'):
                    scv_accession = scv.xpath('./ClinVarAccession')[0].get('Acc')

                    try:
                        scv_clinsig = scv.xpath('./ClinicalSignificance/Description')[0].text
                    except IndexError:
                        scv_clinsig = None

                    # extract all xrefs for disease dbs:
                    xref_by_db = get_xrefs_by_db(scv.xpath('./TraitSet/Trait'))

                    scv_trait_joined = '|'.join(elem.text for elem in scv.xpath('./TraitSet/Trait/Name/ElementValue'))
                    try:
                        evidence_pmids = '|'.join(list(e.text for e in scv.xpath('./ObservedIn/ObservedData/Citation/ID[@Source="PubMed"]')))
                    except IndexError:
                        evidence_pmids = None

                    try:
                        evidence_descriptions = list(e.text for e in scv.xpath('./ObservedIn/ObservedData/Attribute[@Type="Description"]'))
                    except IndexError:
                        evidence_descriptions = None

                    xml_element_clear_memory(scv)
                    rec = {
                        'variation_id': variation_id,
                        'rcv_accession': rcv_accession,
                        'scv_accession': scv_accession,
                        'scv_clinsig': scv_clinsig,
                        'scv_traits': scv_trait_joined,
                        'evidence_pmids': evidence_pmids,
                        'evidence_descriptions': '|'.join(evidence_descriptions),
                        'mesh_ids': '|'.join(xref_by_db['MeSH']),
                        'medgen_ids': '|'.join(xref_by_db['MedGen']),
                        'omim_ids': '|'.join(xref_by_db['OMIM']),
                        'hpo_ids': '|'.join(xref_by_db['HP'])
                    }
                    yield rec
                xml_element_clear_memory(elem)
