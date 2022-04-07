#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mysql.connector

from reports.billing import payments
from reports.billing import abonent
from reports.billing import abonent_addr
from reports.billing import abonent_ident
from reports.billing import ip_plan
from reports.static import pay_type
from reports.static import regions
from reports.static import doc_type
from reports.static import sups

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [%(module)s]: %(message)s')
logging.info("Start full reports")

# connect mysql server
db = mysql.connector.connect(
    host="100.100.10.11",
    user="lbpay",
    password="lbpay",
    database="billing"
)
print(">>>> Connect to db: " + db.database)

doc_type.report()
pay_type.report()
regions.report()
sups.report()

try:
    cursor = db.cursor()
    cursor.callproc('SORM_ABONENTS')
    cursor.close()

    abonent.report_full(db)
    abonent_addr.report(db)
    payments.report_full(db)
    abonent_ident.report_full(db)
    ip_plan.report_full(db)

except mysql.connector.Error as error:
    print("Failed to execute: {}".format(error))

finally:
    db.close()
    print("Done")
