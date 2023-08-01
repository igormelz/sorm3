from utils import writer, cursor, format_filename
from datetime import date, timedelta
import re
import logging

FORMAT = 'SWITCHES_%Y%m%d_%H%M.txt'
FIELDS = [
    'ID',
    'BEGIN_TIME',
    'END_TIME',
    'DESCRIPTION',
    'NETWORK_TYPE',
    'ADDRESS_TYPE_ID',
    'ADDRESS_TYPE',
    'ZIP',
    'COUNTRY',
    'REGION',
    'ZONE',
    'CITY',
    'STREET',
    'BUILDING',
    'BUILD_SECT',
    'APARTMENT',
    'UNSTRUCT_INFO',
    'SWITCH_SIGN',
    'SWITCH_TYPE',
    'REGION_ID'
]

QUERY_FULL = '''
SELECT
	sw.device_id,
	sw.device_name,
	sw.address,
	CONVERT_TZ(sw.last_mod_date,'+03:00','+00:00') begin_time,
	do.value,
	do.archive,
	CONVERT_TZ(do.last_mod_date,'+03:00','+00:00') end_time
from
	devices sw
inner join devices_options do
		using (device_id)
where
	sw.tpl = 0
	and sw.country_id is not null
'''

def report_daily(db):
    return report_full(db, (" AND do.last_mod_date >= (select COALESCE(max(batch_time),current_timestamp) from sorm_batch where batch_name='sw')"))

def report_full(db, params=''):
    with cursor(db) as cur:
        cur.execute(QUERY_FULL + params)
        logging.info("collect srv [{0}]".format(cur.rowcount))
        filename = format_filename(FORMAT)
        with writer(filename, FIELDS) as csvout:
            for row in cur:
                # map out record
                outRow = {
                    'ID': row.get('device_id'),
                    'REGION_ID': 1,  # STSZ
                    'BEGIN_TIME': row.get('begin_time') if row.get('begin_time') < row.get('end_time') else row.get('end_time'),
                    'END_TIME': '' if row.get('archive') == 0 else row.get('end_time'),
                    'DESCRIPTION': row.get('device_name'),
                    'NETWORK_TYPE': 4, # ipV4
                    'ADDRESS_TYPE_ID': 0, # reg
                    'ADDRESS_TYPE': 1, # unstructure
                    'UNSTRUCT_INFO': row.get('address'),
                    'SWITCH_TYPE': 0 # internal
                }
                csvout.writerow(outRow)
        logging.info("flush filename:{0} [{1}]".format(filename, cur.rowcount))
        # store batch info 
        cur.execute("INSERT INTO sorm_batch (batch_name, file_name, file_rec_count) VALUES (%s, %s, %s)", ('sw', filename, cur.rowcount))
        db.commit()
    return filename
