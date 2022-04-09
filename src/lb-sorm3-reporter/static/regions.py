from utils import writer, format_filename

FORMAT = 'REGIONS_%Y%m%d_%H%M.txt'
FIELDS = ['id', 'begin_time', 'end_time', 'description', 'MCC', 'MNC']


def report():
    filename = format_filename(FORMAT)
    with writer(filename, FIELDS) as csvout:
        row = {
            'id': 1,
            'begin_time': '2019-01-01',
            'end_time': '2024-12-31',
            'description': 'Си Телеком Северо-Запад'
        }
        csvout.writerow(row)
    return filename
