from biodb.mysql import MYSQL
from biodb.db import RecordByRecordImportedTable
from biodb.files import LocalFile, ExternalFile
from biodb.io import read_xsv


class StormDetailsOfficial(ExternalFile):
    version = '2011' # StormEvents_details_s20110101_e20111231_c20160115
    # available alternative versions: 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013.


    # note: some years, eg: 2011, 2012 are aggregated into a single file release for details, fatalities, locations
    # whereas later years, eg: 2014 onward have 1 per month for each of the three details, fatalities, locations
    # changover in 2013 lead to a mix for that year. Impact: filename will have to manually be updated as needed.

    @property
    def url(self):
        
        return 'https://www.ncei.noaa.gov/data/storm-events/access/original/{v}/StormEvents_details_s{v}0101_e{v}1231_c20160115'.format(v=self.version)


class StormDetailsFile(LocalFile):
    version = '1'
    depends = [StormDetailsOfficial]
    extension = 'csv'


class StormDetailsTable(RecordByRecordImportedTable):
    version = '1'
    depends = [StormDetailsFile]

    columns = [
        'state',
        'state_fips',
        'year',
        'month_name',
        'event_type  begin_date_time',
        'cz_timezone',
        'end_date_time',
        'injuries_direct',
        'injuries_indirect',
        'deaths_direct',
        'deaths_indirect',
        'damage_property',
        'damage_crops',
        'source',
        'magnitude',
        'magnitude_type',
        'flood_cause',
        'category',
        'tor_f_scale',
        'tor_length',
        'tor_width',
        'episode_title',
        'episode_narrative',
        'event_narrative'
    ]

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                state             VARCHAR(255) NOT NULL,
                state_fips        VARCHAR(255) NOT NULL,
                year              VARCHAR(255) NOT NULL,
                month_name        VARCHAR(255) NOT NULL,
                event_type        VARCHAR(255) NOT NULL,
                begin_date_time   VARCHAR(255) NOT NULL,
                cz_timezone       VARCHAR(255) NOT NULL,
                end_date_time     VARCHAR(255) NOT NULL,
                injuries_direct   VARCHAR(255) NOT NULL,
                injuries_indirect VARCHAR(255) NOT NULL,
                deaths_direct     VARCHAR(255) NOT NULL,
                deaths_indirect   VARCHAR(255) NOT NULL,
                damage_property   VARCHAR(255) NOT NULL,
                damage_crops      VARCHAR(255) NOT NULL,
                source            VARCHAR(255) NOT NULL,
                magnitude         VARCHAR(255) NOT NULL,
                magnitude_type    VARCHAR(255) NOT NULL,
                flood_cause       VARCHAR(255) NOT NULL,
                category          VARCHAR(255) NOT NULL,
                tor_f_scale       VARCHAR(255) NOT NULL,
                tor_length        VARCHAR(255) NOT NULL,
                tor_width         VARCHAR(255) NOT NULL,
                episode_title     VARCHAR(255) NOT NULL,
                episode_narrative VARCHAR(255) NOT NULL,
                event_narrative   VARCHAR(255) NOT NULL,
                INDEX (month_name)
            );
        """

    def read(self):
        for row in read_xsv(self.inputs['StormDetailsFile'].path):
            yield row

