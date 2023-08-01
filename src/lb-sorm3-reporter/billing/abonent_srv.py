from utils import writer, cursor, format_filename
from datetime import date, timedelta
import re
import logging

FORMAT = 'ABONENT_SRV_%Y%m%d_%H%M.txt'
FIELDS = [
    'ABONENT_ID',
    'REGION_ID',
    'ID',
    'BEGIN_TIME',
    'END_TIME',
    'PARAMETER',
    'SRV_CONTRACT',
    'RECORD_ACTION',
    'INTERNAL_ID1',
    'INTERNAL_ID2'
]

QUERY_FULL = '''
SELECT
	v.uid,
	s.contract,
    s.record_action,
	CONVERT_TZ(i.timefrom,'+03:00','+00:00') begin_time,
	CONVERT_TZ(i.timeto,'+03:00','+00:00') end_time
FROM staff_history i
 	INNER JOIN vgroups v ON	(v.vg_id = i.vg_id AND v.id = 2)
	INNER JOIN (SELECT DISTINCT(abonentId) uid, contract, record_action FROM sorm WHERE record_action != 3) s ON (v.uid = s.uid)
'''

def report_daily(db):
    return report_full(db, (" WHERE i.last_mod_date >= (select COALESCE(max(batch_time),current_timestamp) from sorm_batch where batch_name='srv')"
                            " OR EXISTS (SELECT abonentId FROM sorm s WHERE s.abonentId = v.uid AND record_action = 1)"))

def report_full(db, params=''):
    with cursor(db) as cur:
        cur.execute(QUERY_FULL + params)
        logging.info("collect srv [{0}]".format(cur.rowcount))
        filename = format_filename(FORMAT)
        with writer(filename, FIELDS) as csvout:
            for row in cur:
                # map out record
                outRow = {
                    'ABONENT_ID': row.get('uid'),
                    'REGION_ID': 1,  # STSZ
                    'ID': 1, # SUPS_SRV (1) INET
                    'BEGIN_TIME': row.get('begin_time'),
                    'END_TIME': '' if row.get('end_time') == '0000-00-00 00:00:00' else row.get('end_time'),
                    'SRV_CONTRACT': row.get('contract'),
                    'RECORD_ACTION': row.get('record_action')
                }
                csvout.writerow(outRow)
        logging.info("flush filename:{0} [{1}]".format(filename, cur.rowcount))
        # store batch info 
        cur.execute("INSERT INTO sorm_batch (batch_name, file_name, file_rec_count) VALUES (%s, %s, %s)", ('srv', filename, cur.rowcount))
        db.commit()
    return filename
