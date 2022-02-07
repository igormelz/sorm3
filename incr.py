#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import date, timedelta
import mysql.connector

from reports.billing import payments
from reports.billing import ip_plan
from reports.billing import abonent

print("Do INCR reports:", date.today() - timedelta(days=1))

# connect mysql server
db = mysql.connector.connect(
    host="100.100.10.11",
    user="lbpay",
    password="lbpay",
    database="billing"
)
print(">>>> Connect to db: " + db.database)

abonent.report(db, debug=True)
payments.report(db)
ip_plan.report(db)

db.close()
print("Done")
