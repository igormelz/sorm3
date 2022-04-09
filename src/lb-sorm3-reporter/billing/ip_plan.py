from utils import writer, cursor, format_filename
import logging

FORMAT = 'IP_PLAN_%Y%m%d_%H%M.txt'
FIELDS = ['DESCRIPTION', 'ip_type', 'IPV4', 'IPV6', 'IPV4_MASK',
          'IPV6_MASK', 'BEGIN_TIME', 'END_TIME', 'REGION_ID']
QUERY_FULL = "SELECT descr, right(HEX(segment),8) ipv4, right(HEX(mask),8) mv4, date(last_mod_date) dt, archive FROM segments"


def report_daily(db):
    return report_full(db, " WHERE last_mod_date > (select COALESCE(max(batch_time),current_timestamp) from sorm_batch where batch_name='ip_plan')")


def report_full(db, params=''):

    with cursor(db) as cur:
        cur.execute(QUERY_FULL + params)
        filename = format_filename(FORMAT)
        logging.info("collect segments [{0}]".format(cur.rowcount))

        with writer(filename, FIELDS) as csvout:
            for row in cur:
                csvout.writerow({
                    'DESCRIPTION': row.get('descr'),
                    'ip_type': 0,  # IPV4
                    'IPV4': row.get('ipv4'),
                    'IPV4_MASK': row.get('mv4'),
                    'BEGIN_TIME': row.get('dt'),
                    'END_TIME': '2024-12-31' if row.get('archive') == 0 else row.get('dt'),
                    'REGION_ID': 1
                })

        logging.info("flush filename:{0} [{1}]".format(
            filename, cur.rowcount))
        # store batch info
        cur.execute("INSERT INTO sorm_batch (batch_name, file_name, file_rec_count) VALUES (%s, %s, %s)",
                    ('ip_plan', filename, cur.rowcount))
        db.commit()
    return filename
