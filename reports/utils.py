import csv
from datetime import datetime
from contextlib import contextmanager

e164 = lambda p: p[1:] if p[0] == '+' else p

@contextmanager
def writer(FORMAT,FIELDS):
    csvfile = open(datetime.now().strftime(FORMAT), 'w')
    writer = csv.DictWriter(csvfile, FIELDS, delimiter=';', lineterminator='\n')
    writer.writeheader()
    yield writer
    csvfile.close()

@contextmanager
def cursor(connection):
    cur = connection.cursor(buffered=True, dictionary=True)
    yield cur
    cur.close()

