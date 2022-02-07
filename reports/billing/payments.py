from reports.utils import writer, cursor, e164
from datetime import date, timedelta

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
SELECT = ("SELECT pay_date,amount,number,a.uid,name,mobile,address,cash_code "
          "FROM payments p "
          "INNER JOIN agreements a USING(agrm_id) "
          "INNER JOIN accounts u USING(uid) "
          "INNER JOIN accounts_addr d ON(u.uid = d.uid AND d.type = 0)")


def query(full):
    if full == True:
        # return all until today
        return SELECT + " WHERE pay_date < '" + str(date.today()) + "'"
    else:
        # return yesterday
        return SELECT + " WHERE DATE(pay_date) = '" + str(date.today() - timedelta(days=1)) + "'"


def report(db, full=False):
    print("BILLING: PAYMENTS ...... ", end='')
    with cursor(db) as cur:
        cur.execute(query(full))
        with writer(FORMAT, FIELDS) as csvout:
            for dataRowDict in cur:
                # map output record:
                outRow = {
                    'region_id': 1,  # default operator
                    'payment_type': 80,  # bank payment
                    'pay_type_id': dataRowDict.get('cash_code'),
                    'payment_date': dataRowDict.get('pay_date'),
                    'amount': dataRowDict.get('amount'),
                    'amount_CURRENCY': f"{dataRowDict.get('amount'):.2f}",
                    'account': dataRowDict.get('number'),
                    'phone_number': e164(dataRowDict.get('mobile')),
                    'ABONENT_ID': dataRowDict.get('uid'),
                    'address_type_id': 0,  # use registered address
                    'address_type': 1,  # unstructed address
                    'unstruct_info': dataRowDict.get('address'),
                    'RECORD_ACTION': 1,
                }
                csvout.writerow(outRow)
    print("FULL" if full else "INCR")
