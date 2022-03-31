from reports.utils import writer, cursor, format_filename
from datetime import date, timedelta

FORMAT = 'IP_PLAN_%Y%m%d_%H%M.txt'
FIELDS = ['DESCRIPTION', 'ip_type', 'IPV4', 'IPV6', 'IPV4_MASK',
          'IPV6_MASK', 'BEGIN_TIME', 'END_TIME', 'REGION_ID']
QUERY_FULL = "SELECT descr, right(HEX(segment),8) ipv4, right(HEX(mask),8) mv4, date(last_mod_date) dt, archive FROM segments"

def report_full(db):

    with cursor(db) as cur:
        cur.execute(QUERY_FULL)
        filename = format_filename(FORMAT)
        fileRecCount = 0
        with writer(filename, FIELDS) as csvout:
            for row in cur:
                outRow = {
                    'DESCRIPTION': row.get('descr'),
                    'ip_type': 0,  # IPV4
                    'IPV4': row.get('ipv4'),
                    'IPV4_MASK': row.get('mv4'),
                    'BEGIN_TIME': row.get('dt'),
                    'END_TIME': '2024-12-31' if row.get('archive') == 0 else row.get('dt'),
                    'REGION_ID': 1
                }
                csvout.writerow(outRow)
                fileRecCount += 1
        # store batch info 
        cur.execute("INSERT INTO sorm_batch (batch_name, file_name, file_rec_count) VALUES (%s, %s, %s)", ('ip_plan', filename, fileRecCount))
        db.commit()
        print("IP_PLAN: {0} [{1}]".format(filename, fileRecCount))
