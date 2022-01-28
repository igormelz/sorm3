#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import csv
from datetime import datetime

# report name format
FORMAT = 'PAY_TYPE_%Y%m%d_%H%M.csv'
FIELDS = ['ID', 'BEGIN_TIME', 'END_TIME', 'DESCRIPTION', 'REGION_ID']

def report():
    with open(datetime.now().strftime(FORMAT), 'w') as csvout:
        writer = csv.DictWriter(
            csvout, FIELDS, delimiter=';', lineterminator='\n')
        writer.writeheader()
        writer.writerow({'ID': 1, 'BEGIN_TIME': '2019-01-01',
                        'END_TIME': '2024-12-31', 'DESCRIPTION': 'Платеж по эквайрингу', 'REGION_ID': 1})
        writer.writerow({'ID': 2, 'BEGIN_TIME': '2019-01-01',
                         'END_TIME': '2024-12-31', 'DESCRIPTION': 'Платеж по банку', 'REGION_ID': 1})
