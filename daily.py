#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import date, timedelta
import mysql.connector

#from reports.static import pay_type
#from reports.static import regions
#from reports.static import sups
# from reports.billing import ip_plan
from reports.billing import payments
from reports.billing import abonent
from reports.billing import abonent_addr
from reports.billing import abonent_ident

print("Do FULL reports:", date.today() - timedelta(days=1))

# report static
# sups.report()
# pay_type.report()
# regions.report()

# connect mysql server
db = mysql.connector.connect(
    host="100.100.10.11",
    user="lbpay",
    password="lbpay",
    database="billing"
)
db.autocommit = True
print(">>>> Connect to db: " + db.database)

try:
    abonent.report_daily(db)
    abonent_addr.report(db)
    abonent_ident.report_daily(db)
    payments.report_daily(db)

except mysql.connector.Error as error:
    print("Failed to execute: {}".format(error))

finally:
    db.close()
    print("Done")
