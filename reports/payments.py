#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import csv
from datetime import datetime, date
# format phone number - skip leading plus
def e164(p): return p[1:] if p[0] == '+' else p


# report name format
FORMAT = 'PAYMENTS_%Y%m%d_%H%M.csv'
FIELDS = ['region_id', 'payment_type', 'pay_type_id', 'payment_date', 'amount', 'amount_CURRENCY', 'phone_number', 'account', 'ABONENT_ID', 'bank_account', 'bank_name', 'express_card_number', 'terminal_id', 'terminal_number', 'latitude', 'longitude', 'projection_type', 'center_id', 'donated_phone_number',
          'donated_account', 'donated_internal_id1', 'donated_internal_id2', 'card_number', 'pay_params', 'person_recieved', 'bank_division_name', 'bank_card_id', 'address_type_id', 'address_type', 'zip', 'country', 'region', 'zone', 'city', 'street', 'building', 'build_sect', 'apartment', 'unstruct_info', 'RECORD_ACTION']


def report(cursor, full=False):
    ''' payment report '''

    # query prepay
    query = ("SELECT pay_date,amount,number,a.uid,name,mobile,address,cash_code "
             "FROM payments p"
             "INNER JOIN agreements a USING(agrm_id)"
             "INNER JOIN accounts u USING(uid)"
             "INNER JOIN accounts_addr d ON(u.uid = d.uid AND d.type = 0)"
             "WHERE pay_date > DATE(%(limit)s)")

    # exec
    cursor.execute(query, {'limit': date(2022, 1, 23)})
    # print(cursor.column_names)

    # write report
    with open(datetime.now().strftime(FORMAT), 'w') as csvout:
        writer = csv.DictWriter(csvout, FIELDS,
                                delimiter=';', lineterminator='\n')
        writer.writeheader()
        for dataRow in cursor:
            # convert data into dict
            dataRowDict = dict(zip(cursor.column_names, dataRow))
            # define output row
            outRow = {}
            # fill output record:
            # set const for region_id
            outRow['region_id'] = 1
            # set const payment_type
            outRow['payment_type'] = 80
            # use lb cash_code as pay_type_id, i.e. 1-card(cash), 2-bank, 3-sale, 4-refund
            outRow['pay_type_id'] = dataRowDict.get('cash_code')
            # use lb pay_date
            outRow['payment_date'] = dataRowDict.get('pay_date')
            # use lb amount
            outRow['amount'] = dataRowDict.get('amount')
            # use lb amount format 2 places 0.00
            outRow['amount_CURRENCY'] = f"{dataRowDict.get('amount'):.2f}"
            # use lb agreement number
            outRow['account'] = dataRowDict.get('number')
            # use lb phone|mobile
            outRow['phone_number'] = e164(dataRowDict.get('mobile'))
            # use lb account uid
            outRow['ABONENT_ID'] = dataRowDict.get('uid')
            # const 0 = registration address type
            outRow['address_type_id'] = 0
            # const 1 = unstructed address
            outRow['address_type'] = 1
            # use lb address
            outRow['unstruct_info'] = dataRowDict.get('address')
            # add record
            outRow['RECORD_ACTION'] = 1
            # write record
            writer.writerow(outRow)

    cursor.close()
