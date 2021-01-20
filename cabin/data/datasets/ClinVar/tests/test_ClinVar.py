import pytest


@pytest.fixture
def ClinVarVCFTable():
    # HACK to workaround circular imports
    from biodb.data import registry
    return registry.ClinVarVCFTable


def test_get_gene_symbols_and_ids(ClinVarVCFTable):
    result = ClinVarVCFTable()._get_gene_symbols_and_ids('PRKCZ:5590|FAAP20:199990')
    assert result == (['PRKCZ', 'FAAP20'], ['5590', '199990'])


def test_get_dbid_by_dbname(ClinVarVCFTable):
    clndisdb = ('Human_Phenotype_Ontology:HP:0001250,MedGen:C0036572', 'MONDO:MONDO:0001071,MedGen:C1843367,.', '.')
    disease_id_by_db_name = ClinVarVCFTable()._get_dbid_by_dbname(clndisdb)

    # verify correct aggregation of id by names
    db_names = ['Human_Phenotype_Ontology', 'MedGen', 'MONDO']
    assert all([db_name in disease_id_by_db_name for db_name in db_names])
    assert len(disease_id_by_db_name['MedGen']) == 2
    assert '.' not in disease_id_by_db_name

    # verify that ids are as they come, not all prefixes present
    assert disease_id_by_db_name['MedGen'] == {'C0036572', 'C1843367'}
    assert disease_id_by_db_name['MONDO'] == {'MONDO:0001071'}
    assert disease_id_by_db_name['Human_Phenotype_Ontology'] == {'HP:0001250'}


def test_get_MC_SO(ClinVarVCFTable):
    mc_field = ('SO:0001619|non-coding_transcript_variant',
                'SO:0001819|synonymous_variant',
                'SO:0001624|3_prime_UTR_variant')

    result = ClinVarVCFTable()._get_MC_SO(mc_field)
    expected_result = (['non-coding_transcript_variant', 'synonymous_variant', '3_prime_UTR_variant'],
                       ['SO:0001619', 'SO:0001819', 'SO:0001624'])
    assert result == expected_result
