#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import csv
from datetime import datetime

# report name format
FORMAT = 'REGIONS_%Y%m%d_%H%M.csv'
FIELDS = ['id', 'begin_time', 'end_time', 'description', 'MCC', 'MNC']

def report():
    with open(datetime.now().strftime(FORMAT), 'w') as csvout:
        writer = csv.DictWriter(
            csvout, FIELDS, delimiter=';', lineterminator='\n')
        writer.writeheader()
        writer.writerow({'id': 1, 'begin_time': '2019-01-01',
                        'end_time': '2024-12-31', 'description': 'Си Телеком Северо-Запад'})
                        
