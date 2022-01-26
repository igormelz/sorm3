#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
from datetime import datetime, date

e164 = lambda p : p[1:] if p[0] == '+' else p

def report(cursor, full=False):
    ''' payment report '''

    # query prepay
    query = ("SELECT pay_date,amount,number,a.uid,name,mobile,address "
             "FROM payments p"
             "INNER JOIN agreements a USING(agrm_id)"
             "INNER JOIN accounts u USING(uid)"
             "INNER JOIN accounts_addr d ON(u.uid = d.uid AND d.type = 0)"
             "WHERE pay_date > DATE(%(limit)s)")
    # exec
    cursor.execute(query, {'limit': date(2022, 1, 23)})
    print(cursor.column_names)
    with open(datetime.now().strftime('PAYMENTS_%Y%m%d_%H%M.csv'), 'w') as csvout:
        reportFields = ['region_id', 'payment_type', 'pay_type_id', 'payment_date', 'amount', 'amount_CURRENCY', 'phone_number', 'account', 'ABONENT_ID', 'bank_account', 'bank_name', 'express_card_number', 'terminal_id', 'terminal_number', 'latitude', 'longitude', 'projection_type', 'center_id', 'donated_phone_number',
                        'donated_account', 'donated_internal_id1', 'donated_internal_id2', 'card_number', 'pay_params', 'person_recieved', 'bank_division_name', 'bank_card_id', 'address_type_id', 'address_type', 'zip', 'country', 'region', 'zone', 'city', 'street', 'building', 'build_sect', 'apartment', 'unstruct_info', 'RECORD_ACTION']
        writer = csv.DictWriter(csvout, reportFields, delimiter=';')
        writer.writeheader()
        for dataRow in cursor:
            # convert data into dict
            dataRowDict = dict(zip(cursor.column_names, dataRow))
            # define output row
            outRow = {}
            # fill output record
            outRow['region_id'] = 1
            outRow['payment_type'] = 80
            outRow['pay_type_id'] = 1
            outRow['payment_date'] = dataRowDict.get('pay_date')
            outRow['amount'] = dataRowDict.get('amount')
            outRow['amount_CURRENCY'] = dataRowDict.get('amount')
            outRow['account'] = dataRowDict.get('number')
            outRow['phone_number'] = e164(dataRowDict.get('mobile'))
            outRow['ABONENT_ID'] = dataRowDict.get('uid')
            outRow['address_type_id'] = 0
            outRow['address_type'] = 1
            outRow['unstruct_info'] = dataRowDict.get('address')
            outRow['RECORD_ACTION'] = 1
            writer.writerow(outRow)

    cursor.close()
