#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import mysql.connector
import ftplib

#from reports.static import pay_type
#from reports.static import regions
#from reports.static import sups
# from reports.billing import ip_plan
from reports.billing import payments
from reports.billing import abonent
from reports.billing import abonent_addr
from reports.billing import abonent_ident

print("Do daily reports:")

# connect mysql server
db = mysql.connector.connect(
    host="100.100.10.11",
    user="lbpay",
    password="lbpay",
    database="billing"
)
db.autocommit = True
print(">>>> Connect to db: ", db.database)

ftp_server = ftplib.FTP(
        os.environ.get('SORM_HOST', 'localhost'), 
        os.environ.get('SORM_USER', 'sorm'), 
        os.environ.get('SORM_PASS', 'pass')
)
ftp_server.encoding = "utf-8"
print("Connect to ftp:", ftp_server.host)

try:
    filename = abonent.report_daily(db)
    with open(filename, "rb") as file:
        ftp_server.storbinary(f"STOR test/{filename}", file)

    filename = abonent_addr.report(db)
    with open(filename, "rb") as file:
        ftp_server.storbinary(f"STOR test/{filename}", file)

    filename = abonent_ident.report_daily(db)
    with open(filename, "rb") as file:
        ftp_server.storbinary(f"STOR test/{filename}", file)

    filename = payments.report_daily(db)
    with open(filename, "rb") as file:
        ftp_server.storbinary(f"STOR test/{filename}", file)

except mysql.connector.Error as error:
    print("Failed to execute: {}".format(error))

finally:
    db.close()
    ftp_server.quit()
    print("Done")