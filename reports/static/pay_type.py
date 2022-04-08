from reports.utils import writer, format_filename

# report name format
FORMAT = 'PAY_TYPE_%Y%m%d_%H%M.txt'
FIELDS = ['ID', 'BEGIN_TIME', 'END_TIME', 'DESCRIPTION', 'REGION_ID']

def report():
    filename = format_filename(FORMAT)
    with writer(filename, FIELDS) as csvout:
        csvout.writerow({'ID': 1, 'BEGIN_TIME': '2019-01-01',
                        'END_TIME': '2024-12-31', 'DESCRIPTION': 'Платеж по эквайрингу', 'REGION_ID': 1})
        csvout.writerow({'ID': 2, 'BEGIN_TIME': '2019-01-01',
                         'END_TIME': '2024-12-31', 'DESCRIPTION': 'Платеж по банку', 'REGION_ID': 1})
    return filename