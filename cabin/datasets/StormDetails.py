from cabin.db import RecordByRecordImportedTable
from cabin.files import LocalFile, ExternalFile
from cabin.io import read_csv


class StormDetailsOfficial(ExternalFile):
    """ Details of storms in the US, yearly historical data.

    This dataset takes the file of 'details' though there are complementary data
    for 'fatalities' and 'locations'. Data for available versions is aggregated
    by year, all certified and uploaded 20160115. Changeover in 2013 to monthly
    data updates are currently available and maintained through 2021, and ongoing.

    See https://www.ncdc.noaa.gov/stormevents/ftp.jsp for detailed documentation

    available versions: 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013
    """
    version = '2011'

    @property
    def url(self):

        return 'https://www.ncei.noaa.gov/data/storm-events/access/original/{v}/StormEvents_details_s{v}0101_e{v}1231_c20160115.csv'.format(v=self.version)


class StormDetailsFile(LocalFile):
    version = '1'
    depends = [StormDetailsOfficial]
    extension = 'csv'


class StormDetailsTable(RecordByRecordImportedTable):
    version = '1'
    depends = [StormDetailsFile]

    columns = [
        'state',
        'month_name',
        'event_type',
        'begin_date_time',
        'end_date_time',
        'damage_property',
        'damage_crops',
        'magnitude',
        'magnitude_type',
        'category',
        'tor_f_scale',
        'event_narrative'
    ]

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                state             VARCHAR(255) NOT NULL,
                month_name        VARCHAR(255) NOT NULL,
                event_type        VARCHAR(255) NOT NULL,
                begin_date_time   VARCHAR(255) NOT NULL,
                end_date_time     VARCHAR(255) NOT NULL,
                damage_property   VARCHAR(255) NOT NULL,
                damage_crops      VARCHAR(255) NOT NULL,
                magnitude         VARCHAR(255) NOT NULL,
                magnitude_type    VARCHAR(255) NOT NULL,
                category          VARCHAR(255) NOT NULL,
                tor_f_scale       VARCHAR(255) NOT NULL,
                event_narrative           TEXT NOT NULL,
                INDEX (month_name)
            );
        """

    def read(self):
        for row in read_csv(self.inputs['StormDetailsFile'].path):
            yield row
