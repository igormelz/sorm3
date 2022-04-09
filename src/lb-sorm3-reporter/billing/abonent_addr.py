from utils import writer, cursor, format_filename
from datetime import date, timedelta
import logging

FORMAT = 'ABONENT_ADDR_%Y%m%d_%H%M.txt'
FIELDS = [
    'ABONENT_ID',
    'REGION_ID',
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
    'BEGIN_TIME',
    'END_TIME',
    'RECORD_ACTION',
    'INTERNAL_ID1',
    'INTERNAL_ID2'
]

QUERY_FULL = '''
SELECT u.uid, a.address
FROM (SELECT distinct(s.abonentId) AS uid FROM sorm s WHERE record_action = 1 
AND NOT EXISTS (SELECT abonentId FROM sorm p WHERE p.record_action = 4 AND p.abonentId = s.abonentId)) u
INNER JOIN accounts_addr a ON u.uid = a.uid AND a.type = 0
'''

def report(db):

    with cursor(db) as cur:
        cur.execute(QUERY_FULL)
        logging.info("collect addr [{0}]".format(cur.rowcount))
        filename = format_filename(FORMAT)
        with writer(filename, FIELDS) as csvout:
            for row in cur:
                # map out record
                outRow = {
                    'ABONENT_ID': row.get('uid'),
                    'REGION_ID': 1,  # STSZ
                    'ADDRESS_TYPE_ID': 0, # reg addr
                    'ADDRESS_TYPE': 1, # unstructure
                    'UNSTRUCT_INFO': row.get('address'),
                    'BEGIN_TIME': '2019-04-01',
                    'END_TIME': '2024-12-31'
                }
                csvout.writerow(outRow)
        logging.info("flush filename:{0} [{1}]".format(filename, cur.rowcount))      
        # store batch info 
        cur.execute("INSERT INTO sorm_batch (batch_name, file_name, file_rec_count) VALUES (%s, %s, %s)", ('addr', filename, cur.rowcount))
        db.commit()
    return filename

