from biodb.io import read_vcf, read_xsv

from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import LocalFile, ExternalFile


MAX_VARIANT_LENGTH = 250  # biologically relevant to remove large structural variants


class ClinVarVCFOfficial(ExternalFile):
    version = '20201219'

    @property
    def url(self):
        return 'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar_{version}.vcf.gz'.format(version=self.version)


class ClinVarVCFFile(LocalFile):
    version = '1'
    depends = [ClinVarVCFOfficial]
    extension = 'vcf.gz'


class ClinVarVCFTable(RecordByRecordImportedTable):
    version = ' 1'
    depends = [ClinVarVCFFile]

    columns = [
        'chr',
        'pos',
        'ref',
        'alt',
        'variation_id',
        'allele_id',
        'clinical_significance',
        'hpo_ids',
        'medgen_ids',
        'omim_ids',
        'mondo_ids',
        'mesh_ids',
        'molecular_consequences',
        'molecular_consequences_so',
        'gene_symbols',
        'gene_ids']

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                chr                        VARCHAR(255) NOT NULL,      -- chromsome
                pos                        INTEGER      NOT NULL,      -- position in chromsome
                ref                        VARCHAR(255) NOT NULL,      -- nucleotide(s) in reference genome, 250bp max
                alt                        VARCHAR(255) NOT NULL,      -- nucleotide(s) in alternative allele, 250bp max
                variation_id               VARCHAR(255) PRIMARY KEY,   -- ClinVar identifier, e.g. https://www.ncbi.nlm.nih.gov/clinvar/variation/375891/
                allele_id                  VARCHAR(255) NOT NULL,      -- ClinVar Allele ID, complex variation (ie: haplotypes) are not included in vcf
                clinical_significance      LONGTEXT     NOT NULL,      -- Clinical significance for this single variant, as asserted by submitter
                hpo_ids                    VARCHAR(2000) NULL,         -- HPO id from CLNDISDB columns
                medgen_ids                 VARCHAR(855) NULL,          -- MedGen id from CLNDISDB columns
                omim_ids                   VARCHAR(255) NULL,          -- OMIM id from CLNDISDB columns
                mondo_ids                  VARCHAR(655) NULL,          -- MONDO id from CLNDISDB columns
                mesh_ids                   VARCHAR(255) NULL,          -- MeSH id from CLNDISDB columns
                molecular_consequences     VARCHAR(255) NULL,          -- pipe separated list of molecular consequences, ex: missense_variant
                molecular_consequences_so  VARCHAR(255) NULL,          -- pipe separated list of molecular consequence sequence ontologies identifiers, ex: SO:0001583
                gene_symbols               VARCHAR(255) NOT NULL,      -- pipe separated HGNC gene symbol
                gene_ids                   VARCHAR(255) NOT NULL,      -- pipe separated NCBI gene identifier
                INDEX (chr, pos, ref, alt)
            );
        """

    def _fix_clndisdb(self, bad_clndisdb):
        """ ClinVarVCF file fails to comply with vcf specs, pysam parsing is wrong.

        https://samtools.github.io/hts-specs/VCFv4.1.pdf specifies use of commas as
        delimiters for INFO field. ClinVarVCF produces key=CLNDISDB in INFO field with `|` as
        first delimiter to group by phenotype label, then by `,` to list db ids for the label.
        Presently, we reduce this grouping by phenotype and produce a dictionary of all ids per database.

        Args:

            bad_clndisdb (tuple): The original INFO value from CLNDISDB as parsed by pysam

        Returns:

            tuple: The correct parsed value of the same field

        To interpret CLNDISDB, example for id 224718. VCF value for CLNDISDB wrapped on pipe and
        annotated with label:

            .|
            .|
            Human_Phenotype_Ontology:HP:0001249,MONDO:MONDO:0001071,MedGen:C1843367|  # Intellectual disability
            Human_Phenotype_Ontology:HP:0001250,MedGen:C0036572|  # Seizure
            Human_Phenotype_Ontology:HP:0001252,MedGen:C0026827|  # Muscular hypotonia
            Human_Phenotype_Ontology:HP:0001263,MedGen:C0557874|  # Global developmental delay
            Human_Phenotype_Ontology:HP:0002069,MedGen:C0494475   # Bilateral tonic-clonic seizure

        """
        return ','.join(bad_clndisdb).split('|')

    def _get_dbid_by_dbname(self, clndisdb):
        """
        Args:
            tuple: correctly parsed CLNDISDB, as returned by _fix_clndisdb().

        Returns:
            dict: of database name => set of identifiers

        Example output:
            {
                'Human_Phenotype_Ontology': ('HP:0001249', 'HP:0001250', 'HP:0001252', 'HP:0001263', 'HP:0002069'),
                'MONDO': ('MONDO:0001071'),
                'MedGen': ('C1843367', 'C0036572', 'C0026827', 'C0557874', 'C0494475')
            }
        """
        disease_id_by_db_name = {}
        for phenotype in clndisdb:
            # eg '.,Human_Phenotype_Ontology:HP:0001249,MONDO:MONDO:0001071'

            if phenotype == '':
                continue

            for db_entry in phenotype.split(','):
                if db_entry == '.':
                    continue

                # e.g. ('Human_Phenotype_Ontology', 'HP:0001249')
                db_name, db_id = db_entry.split(':', 1)

                if db_name not in disease_id_by_db_name:
                    disease_id_by_db_name[db_name] = set()
                disease_id_by_db_name[db_name].add(db_id)

        return disease_id_by_db_name

    def _get_MC_SO(self, mc_field):
        """
        return the word and sequence ontology id of all Molecular Consequences.
        eg SO: http://www.sequenceontology.org/browser/current_svn/term/SO:0001619

        Args:
            mc_field (tuple): pysam-parsed MC INFO tag

        Returns
            a tuple of two lists: molecular consequences and corresponding SO ids.

        Example input:
            ('SO:0001619|non-coding_transcript_variant',
             'SO:0001819|synonymous_variant',
             'SO:0001624|3_prime_UTR_variant')

        Example output:
            (['non-coding_transcript_variant', 'synonymous_variant', '3_prime_UTR_variant'],
             ['SO:0001619', 'SO:0001819', 'SO:0001624'])
        """
        if len(mc_field) == 0:
            return ([], [])

        mc_list, so_list = [], []
        for mc_entry in mc_field:
            # e.g. 'SO:0001619|non-coding_transcript_variant',
            so, mc = mc_entry.split('|')
            so_list.append(so)
            mc_list.append(mc)
        return mc_list, so_list

    def _get_gene_symbols_and_ids(self, geneinfo_field):
        """
        return the symbols and ids of all genes affected by variant.
        Note: ClinVar VCF wrongly delcares Number=1 for GENEINFO, we get it as
        a "|"-delimited string.
        Args:
            input (str): "|"-separated list of 'SYMBOL:ID', for example:
                         'PRKCZ:5590|FAAP20:199990'

        Returns:
            tuple: of two lists, one for gene symbols, one for gene IDs.

        Example output: (['PRKCZ', 'FAAP20'], ['5590', '199990'])
        """
        if geneinfo_field == '':
            return [], []

        gene_symbols, gene_ids = [], []
        for info_pair in geneinfo_field.split('|'):
            # e.g. "PRKCZ:5590"
            gene_symbol, gene_id = info_pair.split(':')

            gene_symbols.append(gene_symbol)
            gene_ids.append(gene_id)

        return gene_symbols, gene_ids

    def read(self):
        for row in read_vcf(self.input.path):
            # Populate basic fields directly from row, note: one alt only in ClinVarVCF
            chrom, pos, ref, alt = row['CHROM'], row['POS'], row['REF'], row['ALT']
            variation_id = row['ID']
            allele_id = row['INFO'].get('ALLELEID')

            # Process: drop variant with no REF/ALT: some variants lack data, eg. id: 836156
            if not alt or not ref:
                # print("Variant missing ref or alt, skipping id: ", row['ID'])
                continue
            else:
                alt = alt[0]  # alt is tuple of one, eg: ('A',)

            # Process: variants that are long indels are dropped
            if max(len(alt), len(ref)) > MAX_VARIANT_LENGTH:
                continue

            # Populate gene symbols and id
            geneinfo_field = row['INFO'].get('GENEINFO', '')
            gene_symbols, gene_ids = self._get_gene_symbols_and_ids(geneinfo_field)

            # Populate disease cross reference, as needed by db name:
            clndisdb = self._fix_clndisdb(row['INFO'].get('CLNDISDB', ''))
            disease_id_by_db_name = self._get_dbid_by_dbname(clndisdb)

            hpo_ids = disease_id_by_db_name.get('Human_Phenotype_Ontology', [])
            medgen_ids = disease_id_by_db_name.get('MedGen', [])
            omim_ids = disease_id_by_db_name.get('OMIM', [])
            mondo_ids = disease_id_by_db_name.get('MONDO', [])
            mesh_ids = disease_id_by_db_name.get('MeSH', [])

            # Populate molecular consequence
            mc_field = row['INFO'].get('MC', tuple())
            molecular_consequences, consequence_SOs = self._get_MC_SO(mc_field)

            # Populate clinical significance
            clnsig = row['INFO'].get('CLNSIG', [])

            # Create record
            rec = {
                'chr': chrom,
                'pos': pos,
                'ref': ref,
                'alt': alt,
                'variation_id': variation_id,
                'allele_id': allele_id,
                'clinical_significance': '|'.join(clnsig),
                'hpo_ids': '|'.join(hpo_ids),
                'medgen_ids': '|'.join(['MedGen:' + i for i in medgen_ids]),  # eg: MedGen:C0024776
                'omim_ids': '|'.join(['OMIM:' + i for i in omim_ids]),  # Modify to match search term, eg: OMIM:193300
                'mondo_ids': '|'.join(mondo_ids),
                'mesh_ids': '|'.join(['MESH:' + i for i in mesh_ids]),  # eg: MESH:D006623
                'molecular_consequences': '|'.join(molecular_consequences),
                'molecular_consequences_so': '|'.join(consequence_SOs),
                'gene_symbols': '|'.join(gene_symbols),
                'gene_ids': '|'.join(gene_ids)
            }
            yield rec

