#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, date
from reports import payments
import mysql.connector

# connect mysql server
cnx = mysql.connector.connect(
  host = "100.100.10.11",
  user = "lbpay",
  password = "lbpay",
  database = "billing"
)

payments.report(cnx.cursor(buffered=True), full=True)

print("Done")