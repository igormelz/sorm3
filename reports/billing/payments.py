from reports.utils import writer, cursor, format_filename
from datetime import date, timedelta
import re

FORMAT = 'PAYMENTS_%Y%m%d_%H%M.txt'
FIELDS = [
    'region_id',
    'payment_type',
    'pay_type_id',
    'payment_date',
    'amount',
    'amount_CURRENCY',
    'phone_number',
    'account',
    'ABONENT_ID',
    'bank_account',
    'bank_name',
    'express_card_number',
    'terminal_id',
    'terminal_number',
    'latitude',
    'longitude',
    'projection_type',
    'center_id',
    'donated_phone_number',
    'donated_account',
    'donated_internal_id1',
    'donated_internal_id2',
    'card_number',
    'pay_params',
    'person_recieved',
    'bank_division_name',
    'bank_card_id',
    'address_type_id',
    'address_type',
    'zip',
    'country',
    'region',
    'zone',
    'city',
    'street',
    'building',
    'build_sect',
    'apartment',
    'unstruct_info',
    'RECORD_ACTION'
]

QUERY_FULL = '''
SELECT CONVERT_TZ(p.pay_date,'+03:00','+00:00') as pay_date, p.amount, a.contract, a.abonentId, u.name, ifnull(u.mobile,'') mobile, d.address, p.cash_code 
FROM sorm a
INNER JOIN payments p ON a.contractId = p.agrm_id 
INNER JOIN accounts u ON u.uid = a.abonentId 
INNER JOIN accounts_addr d ON(u.uid = d.uid AND d.type = 0)
WHERE a.record_action != 3
'''

AGG_PATTERN = re.compile(r'-.*')
PHONE_PATTERN = re.compile(r'^\+')

def report_daily(db):
    report_full(db, " AND p.pay_date >= (select COALESCE(max(batch_time),current_timestamp) from sorm_batch where batch_name='payment')")

def report_full(db, params=''):
    with cursor(db) as cur:
        cur.execute(QUERY_FULL + params)
        print(">>>> query full payments [{0}]".format(cur.rowcount))
        filename = format_filename(FORMAT)
        with writer(filename, FIELDS) as csvout:
            for dataRowDict in cur:
                # map output record:
                outRow = {
                    'region_id': 1,  # default operator
                    'payment_type': 80,  # bank payment
                    'pay_type_id': dataRowDict.get('cash_code'),
                    'payment_date': dataRowDict.get('pay_date'),
                    'amount': dataRowDict.get('amount'),
                    'amount_CURRENCY': f"{dataRowDict.get('amount'):.2f}",
                    'account': AGG_PATTERN.sub('', dataRowDict.get('contract')),
                    # 'phone_number': PHONE_PATTERN.sub('', dataRowDict.get('mobile')),
                    'phone_number': '790000000',
                    'ABONENT_ID': dataRowDict.get('abonentId'),
                    'address_type_id': 0,  # use registered address
                    'address_type': 1,  # unstructed address
                    'unstruct_info': dataRowDict.get('address'),
                    'RECORD_ACTION': 1,
                }
                csvout.writerow(outRow)
        # store batch info 
        print("PAYMENTS: {0} [{1}]".format(filename, cur.rowcount))
        cur.execute("INSERT INTO sorm_batch (batch_name, file_name, file_rec_count) VALUES (%s, %s, %s)", ('payment', filename, cur.rowcount))
        db.commit()

