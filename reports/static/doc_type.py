from reports.utils import writer

# report name format
FORMAT = 'DOC_TYPE_%Y%m%d_%H%M.txt'
FIELDS = ['DOC_TYPE_ID', 'BEGIN_TIME', 'END_TIME', 'DESCRIPTION', 'REGION_ID']

def report():
    print("STATIC: DOC_TYPE ........ ", end='')
    with writer(FORMAT, FIELDS) as csvout:
        csvout.writerow({'DOC_TYPE_ID': 1, 'BEGIN_TIME': '2019-01-01',
                        'END_TIME': '2024-12-31', 'DESCRIPTION': 'Паспорт', 'REGION_ID': 1})
    print("OK")