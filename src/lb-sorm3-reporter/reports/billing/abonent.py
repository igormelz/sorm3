from reports.utils import writer, cursor, cursorDef, format_filename
from datetime import date
import re
import logging

FORMAT = 'ABONENT_%Y%m%d_%H%M.txt'
FIELDS = [
    'ID',
    'REGION_ID',
    'CONTRACT_DATE',
    'CONTRACT',
    'ACCOUNT',
    'ACTUAL_FROM',
    'ACTUAL_TO',
    'ABONENT_TYPE',
    'NAME_INFO_TYPE',
    'FAMILY_NAME',
    'GIVEN_NAME',
    'INITIAL_NAME',
    'UNSTRUCT_NAME',
    'BIRTH_DATE',
    'IDENT_CARD_TYPE_ID',
    'IDENT_CARD_TYPE',
    'IDENT_CARD_SERIAL',
    'IDENT_CARD_NUMBER',
    'IDENT_CARD_DESCRIPTION',
    'IDENT_CARD_UNSTRUCT',
    'BANK',
    'BANK_ACCOUNT',
    'FULL_NAME',
    'INN',
    'CONTACT',
    'PHONE_FAX',
    'STATUS',
    'ATTACH',
    'DETACH',
    'NETWORK_TYPE',
    'RECORD_ACTION',
    'INTERNAL_ID1'
]

AGG_PATTERN = re.compile(r'-.*')
ANON_NUM = re.compile(r'\d')
ANON_NAME = re.compile(r'(\w)\w+')

QUERY_FULL = '''
SELECT a.contractDate, a.contract, a.contractState, a.record_action,
CONVERT_TZ(a.attach,'+03:00','+00:00') attach, CONVERT_TZ(a.detach,'+03:00','+00:00') detach,
u.uid, u.type, replace(u.name,'"','') AS name,
u.birthdate, u.sole_proprietor, u.pass_sernum, u.pass_no,
concat(u.pass_issuedate, ' ', u.pass_issueplace) pass_descr,
replace(u.bank_name,'"','') AS bank_name, u.inn, u.settl, u.gen_dir_u, u.phone, u.doc_type
FROM sorm a
INNER JOIN accounts u ON a.abonentId = u.uid
WHERE a.record_action < 3
'''

def report(db):
    ''' report '''
    with cursor(db) as cur:
        cur.execute(QUERY_FULL)
        logging.info("collect contracts [{0}]".format(cur.rowcount))

        filename = format_filename(FORMAT)
        with writer(filename, FIELDS) as csvout:
            for row in cur:
                out = {
                    'ID': row.get('uid'),
                    'REGION_ID': 1,
                    'CONTRACT_DATE': row.get('contractDate'),
                    'CONTRACT': AGG_PATTERN.sub('', row.get('contract')),
                    'STATUS': '0' if row.get('contractState') == 0 else '1',
                    'ATTACH': row.get('attach'),
                    'DETACH': row.get('detach') if row.get('contractState') != 0 else '',
                    'ACTUAL_FROM': '2019-04-01',
                    'ACTUAL_TO': str(date.today()),
                    'NETWORK_TYPE': 4,
                    'RECORD_ACTION': row.get('record_action')
                }
                if(row.get('type') == 2 and row.get('sole_proprietor') == 0):
                    out['ABONENT_TYPE'] = "42"
                    out['NAME_INFO_TYPE'] = 1  # unstructed name
                    out['UNSTRUCT_NAME'] = row.get('name')
                    # out['UNSTRUCT_NAME'] = ANON_NAME.sub(r'\1*', row.get('name'))  # fix!!!
                    out['BIRTH_DATE'] = row.get('birthdate')
                    # out['BIRTH_DATE'] = '1990-01-01'  # delete
                    out['IDENT_CARD_TYPE_ID'] = row.get('doc_type')
                    out['IDENT_CARD_TYPE'] = 0  # structed
                    # out['IDENT_CARD_SERIAL'] = ANON_NUM.sub('0', row.get('pass_sernum'))  # fix!!!
                    out['IDENT_CARD_SERIAL'] = row.get('pass_sernum')
                    # out['IDENT_CARD_NUMBER'] = ANON_NUM.sub('0', row.get('pass_no'))  # fix!!!
                    out['IDENT_CARD_NUMBER'] = row.get('pass_no')
                    out['IDENT_CARD_DESCRIPTION'] = row.get('pass_descr')
                else:
                    out['ABONENT_TYPE'] = "43"
                    out['FULL_NAME'] = row.get('name')
                    out['INN'] = row.get('inn')
                    out['CONTACT'] = row.get('gen_dir_u')
                    out['PHONE_FAX'] = row.get('phone')
                    out['BANK'] = row.get('bank_name')
                    out['BANK_ACCOUNT'] = row.get('settl')
                csvout.writerow(out)

        logging.info("flush filename:{0} [{1}]".format(filename, cur.rowcount))
        # store batch info
        cur.execute("INSERT INTO sorm_batch (batch_name, file_name, file_rec_count) VALUES (%s, %s, %s)",
                    ('abonent', filename, cur.rowcount))
        db.commit()
    return filename
