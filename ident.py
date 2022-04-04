#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mysql.connector
import ftplib

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
print(">>>> Connect to db: " , db.database)

ftp_server = ftplib.FTP("10.200.10.95", "igorm", "Ium_7403010")
ftp_server.encoding = "utf-8"
print("Connect to ftp:", ftp_server.host)

try:
    filename = payments.report_daily(db)
    with open(filename, "rb") as file:
        ftp_server.storbinary(f"STOR test/{filename}", file)

except mysql.connector.Error as error:
    print("Failed to execute: {}".format(error))

finally:
    db.close()
    ftp_server.quit()
    print("Done")
