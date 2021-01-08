

def get_xrefs_by_db(xpath_traits):
    """
    Args:
        xpath object: for all traits, eg: for rcv elements at ./ReferenceClinVarAssertion/TraitSet/Trait

    Returns:
        dict: of database name => set of identifiers
    """
    PREFIX_BY_DBNAME = {
        'MedGen': 'MedGen',
        'Human Phenotype Ontology': 'HP',  # RCV elements use full db name
        'HP': 'HP',  # SCV elements use HP as db name, eg: variation id 253832
        'OMIM': 'OMIM',
        'MONDO': 'MONDO',
        'MeSH': 'MeSH',
    }

    # initialize xref for known dbs
    xref_by_db = {k: set() for k in PREFIX_BY_DBNAME}
    for trait in xpath_traits:
        for xref in trait.xpath('./XRef'):
            db_name = xref.get('DB')
            db_id = xref.get('ID')
            db_id_type = xref.get('Type', '')

            # ignore irrelevant dbs
            if db_name not in PREFIX_BY_DBNAME:
                continue

            # ignore secondary id for rcv: RCV000414953
            # eg:'HP:0001307' for primary id : 'HP:0001270'
            if db_id_type == 'secondary':
                continue

            # add prefix if needed:
            if not db_id.startswith(PREFIX_BY_DBNAME[db_name]):
                db_id = PREFIX_BY_DBNAME[db_name] + ":" + db_id

            xref_by_db[db_name].add(db_id)
    return xref_by_db
