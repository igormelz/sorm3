#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from reports import *
import mysql.connector

# connect mysql server
cnx = mysql.connector.connect(
  host = "100.100.10.11",
  user = "lbpay",
  password = "lbpay",
  database = "billing"
)

pay_type.report()
payments.report(cnx.cursor(buffered=True), full=True)
regions.report()

print("Done")