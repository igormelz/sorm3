from reports.utils import writer, format_filename

FORMAT = 'SUPPLEMENTARY_SERVICE_%Y%m%d_%H%M.txt'
FIELDS = ['ID', 'MNEMONIC', 'BEGIN_TIME', 'END_TIME', 'DESCRIPTION', 'REGION_ID']


def report():
    filename = format_filename(FORMAT)
    with writer(filename, FIELDS) as csvout:
        row = {
            'ID': 0,
            'MNEMONIC': 'internet',
            'BEGIN_TIME': '2019-01-01',
            'END_TIME': '2024-12-31',
            'DESCRIPTION': 'Доступ в Интернет',
            'REGION_ID': 1
        }
        csvout.writerow(row)
    return filename
