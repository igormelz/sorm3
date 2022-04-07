#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import mysql.connector
import ftplib
import logging

from reports.billing import payments
from reports.billing import abonent
from reports.billing import abonent_addr
from reports.billing import abonent_ident


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [%(module)s]: %(message)s')
logging.info("Start daily reports")

# connect mysql server
db = mysql.connector.connect(
    host="localhost",
    user="lbpay",
    password="lbpay",
    database="billing"
)
db.autocommit = True
logging.info(f"connect to db: {db.database}")

ftp_server = ftplib.FTP('100.100.10.6', 'igorm', 'Ium_7403010')
ftp_server.encoding = "utf-8"
logging.info(f"connect to ftp: {ftp_server.host}")

try:
    filename = abonent.report_daily(db)
    with open(filename, "rb") as file:
        ftp_server.storbinary(f"STOR test/{filename}", file)
        logging.info(f"sent filename:{filename} to remote host:{ftp_server.host}")

    filename = abonent_addr.report(db)
    with open(filename, "rb") as file:
        ftp_server.storbinary(f"STOR test/{filename}", file)
        logging.info(f"sent filename:{filename} to remote host:{ftp_server.host}")

    filename = abonent_ident.report_daily(db)
    with open(filename, "rb") as file:
        ftp_server.storbinary(f"STOR test/{filename}", file)
        logging.info(f"sent filename:{filename} to remote host:{ftp_server.host}")

    filename = payments.report_daily(db)
    with open(filename, "rb") as file:
        ftp_server.storbinary(f"STOR test/{filename}", file)
        logging.info(f"sent filename:{filename} to remote host:{ftp_server.host}")

except mysql.connector.Error as error:
    logging.error(f"failed to execute: {error}")

finally:
    db.close()
    ftp_server.quit()
    logging.info("end daily reports")
