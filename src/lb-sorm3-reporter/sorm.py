#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import mysql.connector
import ftplib
import logging

from reports.static import pay_type
from reports.static import regions
from reports.static import doc_type
from reports.static import sups

from reports.billing import daily
from reports.billing import payments
from reports.billing import abonent
from reports.billing import abonent_addr
from reports.billing import abonent_ident
from reports.billing import ip_plan

from reports.utils import cursorDef

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s [%(module)s]: %(message)s')

# connect to ftp server
ftp_server = ftplib.FTP(
    host=os.getenv('SORM_HOST'),
    user=os.getenv('SORM_USER'),
    passwd=os.getenv('SORM_PASS')
)
ftp_server.encoding = "utf-8"
logging.info(f"connect to ftp: {ftp_server.host}")

# connect mysql server
db = mysql.connector.connect(
    host=os.getenv('SORM_DB_HOST'),
    user=os.getenv('SORM_DB_USER'),
    password=os.getenv('SORM_DB_USER'),
    database=os.getenv('SORM_DB_NAME')
)
db.autocommit = True
logging.info(f"connect to db: {db.database}")


def upload(filename):
    with open(filename, "rb") as file:
        try:
            ftp_server.storbinary(f"STOR test/{filename}", file)
            logging.info(
                f"sent filename:{filename} to remote host:{ftp_server.host}")
        except ftplib.Error as ftp_error:
            logging.error(f"failed to upload: {error}")

def full_reports():
    try:
        logging.info("start full reports")
        
        # process static
        upload(doc_type.report())
        upload(pay_type.report())
        upload(regions.report())
        upload(sups.report())

        # call stored proc 
        with cursorDef(db) as cursor:
            cursor.callproc('SORM_ABONENTS')
            logging.info("call procedure to reset all tables")

        upload(abonent.report(db))
        upload(abonent_addr.report(db))
        upload(abonent_ident.report_full(db))
        upload(payments.report_full(db))
        upload(ip_plan.report_full(db))

    except mysql.connector.Error as error:
        logging.error(f"failed to execute: {error}")
    except OSError as error:
        logging.error(f"failed to execute: {error}")

    finally:
        db.close()
        ftp_server.quit()
        logging.info("end full reports")

def daily_reports():
    try:
        logging.info("start daily reports")

        # preprocess daily contracts
        daily.pre_process(db)

        # process reports:
        upload(ip_plan.report_daily(db))
        upload(abonent.report(db))
        upload(abonent_addr.report(db))
        upload(abonent_ident.report_daily(db))
        upload(payments.report_daily(db))

    except mysql.connector.Error as error:
        logging.error(f"failed to execute: {error}")
    except OSError as error:
        logging.error(f"failed to execute: {error}")

    finally:
        db.close()
        ftp_server.quit()
        logging.info("end daily reports")


if __name__ == "__main__":
    # get params
    mode = sys.argv[1] if len(sys.argv[1:]) > 0 else 'daily'
    if mode == 'full':
        full_reports()
    else:
        daily_reports()
