#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import date, timedelta
import mysql.connector

from reports.static import pay_type
from reports.billing import payments
from reports.static import regions
from reports.billing import ip_plan

print("Do FULL reports:", date.today() - timedelta(days=1))

# report static
pay_type.report()
regions.report()

# connect mysql server
db = mysql.connector.connect(
    host="100.100.10.11",
    user="lbpay",
    password="lbpay",
    database="billing"
)
print(">>>> Connect to db: " + db.database)

payments.report(db)
ip_plan.report(db,full=True)

db.close()
print("Done")
