from reports.utils import writer, format_filename

# report name format
FORMAT = 'DOC_TYPE_%Y%m%d_%H%M.txt'
FIELDS = [
    'DOC_TYPE_ID',
    'BEGIN_TIME',
    'END_TIME',
    'DESCRIPTION',
    'REGION_ID'
]


def report():
    filename = format_filename(FORMAT)
    with writer(filename, FIELDS) as csvout:
        csvout.writerow({
            'DOC_TYPE_ID': 0,
            'BEGIN_TIME': '2019-01-01',
            'END_TIME': '2024-12-31',
            'DESCRIPTION': 'Паспорт гражданина РФ',
            'REGION_ID': 1
        })
        csvout.writerow({
            'DOC_TYPE_ID': 1,
            'BEGIN_TIME': '2019-01-01',
            'END_TIME': '2024-12-31',
            'DESCRIPTION': 'Заграничный паспорт РФ',
            'REGION_ID': 1
        })
        csvout.writerow({
            'DOC_TYPE_ID': 2,
            'BEGIN_TIME': '2019-01-01',
            'END_TIME': '2024-12-31',
            'DESCRIPTION': 'Удостоверение ВС РФ',
            'REGION_ID': 1
        })
        csvout.writerow({
            'DOC_TYPE_ID': 3,
            'BEGIN_TIME': '2019-01-01',
            'END_TIME': '2024-12-31',
            'DESCRIPTION': 'Паспорт иностранного гражданина',
            'REGION_ID': 1
        })
        csvout.writerow({
            'DOC_TYPE_ID': 4,
            'BEGIN_TIME': '2019-01-01',
            'END_TIME': '2024-12-31',
            'DESCRIPTION': 'Временное удостоверение личности гражданина РФ',
            'REGION_ID': 1
        })
    return filename
