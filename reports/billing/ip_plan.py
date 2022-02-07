from reports.utils import writer, cursor, e164
from datetime import date, timedelta

FORMAT = 'IP_PLAN_%Y%m%d_%H%M.txt'
FIELDS = ['DESCRIPTION', 'ip_type', 'IPV4', 'IPV6', 'IPV4_MASK',
          'IPV6_MASK', 'BEGIN_TIME', 'END_TIME', 'REGION_ID']
select = (
    "SELECT descr, right(HEX(segment),8) ipv4, right(HEX(mask),8) mv4, date(last_mod_date) dt, archive "
    "FROM segments ")


def query(full):
    if full == True:
        # return all until today
        return select + "WHERE archive = 0 and last_mod_date < '" + str(date.today()) + "'"
    else:
        # return yesterday
        return select + "WHERE DATE(last_mod_date) = '" + str(date.today() - timedelta(days=1)) + "'"


def isActive(row):
    if(int(row.get('archive')) == 0):
        return '2024-12-31'
    else:
        return row.get('dt')


def report(db, full=False, debug=False):
    print("BILLING: IP_PLAN ....... ", end='')
    with cursor(db) as cur:
        cur.execute(query(full))
        with writer(FORMAT, FIELDS) as csvout:
            for row in cur:
                if debug:
                    print(row)
                # map out record
                outRow = {
                    'DESCRIPTION': row.get('descr'),
                    'ip_type': 0,  # IPV4
                    'IPV4': row.get('ipv4'),
                    'IPV4_MASK': row.get('mv4'),
                    'BEGIN_TIME': row.get('dt'),
                    'END_TIME': isActive(row),
                    'REGION_ID': 1
                }
                csvout.writerow(outRow)
    print("FULL" if full else "INCR")
