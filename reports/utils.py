import csv
from datetime import datetime
from contextlib import contextmanager

def format_filename(FORMAT):
    return datetime.now().strftime(FORMAT)

@contextmanager
def writer(filename,FIELDS):
    csvfile = open(filename, 'w')
    writer = csv.DictWriter(csvfile, FIELDS, delimiter=';', lineterminator='\n')
    writer.writeheader()
    yield writer
    csvfile.close()

@contextmanager
def cursor(connection):
    cur = connection.cursor(buffered=True, dictionary=True)
    yield cur
    cur.close()

@contextmanager
def cursorDef(connection):
    cur = connection.cursor()
    yield cur
    cur.close()
