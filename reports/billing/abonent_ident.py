from reports.utils import writer, cursor, format_filename
from datetime import date, timedelta
import re
import logging

MAC_PATTERN = re.compile(r':')
LOGIN_PATTERN = re.compile(r'-\d{4}-\d{2}-\d{2}[_ ]\d{2}:\d{2}:\d{2}')

FORMAT = 'ABONENT_IDENT_%Y%m%d_%H%M.txt'
FIELDS = [
    'ABONENT_ID',
    'REGION_ID',
    'IDENT_TYPE',
    'PHONE',
    'INTERNAL_NUMBER',
    'IMSI',
    'IMEI',
    'ICC',
    'MIN',
    'ESN',
    'EQUIPMENT_TYPE',
    'MAC',
    'VPI',
    'VCI',
    'LOGIN',
    'E_MAIL',
    'PIN',
    'USER_DOMAIN',
    'RESERVED',
    'ORIGINATOR_NAME',
    'IP_TYPE',
    'IPV4',
    'IPV6',
    'IPV4_MASK',
    'IPV6_MASK',
    'BEGIN_TIME',
    'END_TIME',
    'LINE_OBJECT',
    'LINE_CROSS',
    'LINE_BLOCK',
    'LINE_PAIR',
    'LINE_RESERVED',
    'LOC_TYPE',
    'LOC_LAC',
    'LOC_CELL',
    'LOC_TA',
    'LOC_CELL_WIRELESS',
    'LOC_MAC',
    'LOC_LATITUDE',
    'LOC_LONGITUDE',
    'LOC_PROJECTION_TYPE',
    'RECORD_ACTION',
    'INTERNAL_ID1',
    'INTERNAL_ID2'
]

QUERY_FULL = '''
SELECT v.uid, v.login,  
right(HEX(i.segment),8) ipv4, right(HEX(i.mask),8) ipv4_mask, 
CONVERT_TZ(i.timefrom,'+03:00','+00:00') begin_time, 
CONVERT_TZ(i.timeto,'+03:00','+00:00') end_time,
(SELECT login FROM radius.sessionsradius r WHERE r.vg_id = v.vg_id) mac
FROM staff_history i
INNER JOIN vgroups v ON (v.vg_id = i.vg_id AND v.id = 2) 
INNER JOIN (SELECT DISTINCT(abonentId) uid FROM sorm WHERE record_action !=3) s ON (v.uid = s.uid)
'''

def report_daily(db):
    return report_full(db, (" WHERE i.last_mod_date >= (select COALESCE(max(batch_time),current_timestamp) from sorm_batch where batch_name='ident')"
                            " OR EXISTS (SELECT abonentId FROM sorm s WHERE s.abonentId = v.uid AND record_action = 1)"))

def report_full(db, params=''):
    with cursor(db) as cur:
        cur.execute(QUERY_FULL + params)
        logging.info("collect ident [{0}]".format(cur.rowcount))
        filename = format_filename(FORMAT)
        with writer(filename, FIELDS) as csvout:
            for row in cur:
                # map out record
                outRow = {
                    'ABONENT_ID': row.get('uid'),
                    'REGION_ID': 1,  # STSZ
                    'IDENT_TYPE': 5, # data
                    'EQUIPMENT_TYPE': 0,  # mac 
                    'MAC': '' if row.get('mac') == None else MAC_PATTERN.sub('', row.get('mac')),
                    'LOGIN': LOGIN_PATTERN.sub('', row.get('login')),
                    'IP_TYPE': 0,  # ipv4
                    'IPV4': row.get('ipv4'),
                    'IPV4_MASK': row.get('ipv4_mask'),
                    'BEGIN_TIME': row.get('begin_time'),
                    'END_TIME': '' if row.get('end_time') == '0000-00-00 00:00:00' else row.get('end_time')
                }
                csvout.writerow(outRow)
        logging.info("flush filename:{0} [{1}]".format(filename, cur.rowcount))
        # store batch info 
        cur.execute("INSERT INTO sorm_batch (batch_name, file_name, file_rec_count) VALUES (%s, %s, %s)", ('ident', filename, cur.rowcount))
        db.commit()
    return filename
