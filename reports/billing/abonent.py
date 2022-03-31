from reports.utils import writer, cursor, cursorDef, format_filename
from datetime import date
import re

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

QUERY_AGRM = '''
SELECT a.uid, a.agrm_id, a.number, a.state, a.date, a.last_mod_date,
s.contractState, s.hasReason,
u.`type`, u.sole_proprietor, CHAR_LENGTH(u.inn) inn, CHAR_LENGTH(u.pass_no) pn, CHAR_LENGTH(u.pass_sernum) sn
FROM agreements a 
LEFT OUTER JOIN sorm s ON (a.agrm_id = s.contractId) 
INNER JOIN accounts u ON(a.uid = u.uid AND u.uid > 20)
WHERE a.last_mod_date > (SELECT max(batch_time) FROM sorm_batch WHERE batch_name ='abonent')  
'''

DEL_CLOSE = 'DELETE FROM sorm WHERE record_action = 1 and contractState = 2'
DEL_UPDATE = 'DELETE FROM sorm WHERE record_action = 2'
UPD_BATCH  = 'UPDATE sorm SET record_action = 4 WHERE record_action = 1'
UPD_CONTRACT = 'UPDATE sorm SET record_action = 2, last_mod_time = CURRENT_TIMESTAMP() WHERE contractId = %(contractId)s'


def report_daily(db):
    ''' report daily '''

    # preprocess 
    with cursorDef(db) as cur:
        # do preprocessing
        cur.execute(DEL_CLOSE)
        print(">>>> drop close contract [{0}]".format(cur.rowcount))
        cur.execute(DEL_UPDATE)
        print(">>>> drop remove record [{0}]".format(cur.rowcount))
        cur.execute(UPD_BATCH)
        print(">>>> mark record as processed [{0}]".format(cur.rowcount))

    with cursor(db) as cur:

        # get new and changed contracts
        cur.execute(QUERY_AGRM)
        print(">>>> query contracts [{0}]".format(cur.rowcount))

        # process contracts
        for agrm in cur:
            if agrm.get('contractState') == None: 
                print(agrm.get('number') + " new: ", end='')
                ret = cur.callproc('SORM_PROC', (agrm.get('agrm_id'), agrm.get('state'), agrm.get('date'), agrm.get('number'),
                                                 agrm.get('uid'), agrm.get('type'), agrm.get('sole_proprietor'), agrm.get('inn'), agrm.get('pn'), agrm.get('sn'), 1))
                print('add' if ret.get('@_SORM_PROC_arg11') == 2 else 'drop')
                db.commit()
            elif agrm.get('hasReason') == 1: 
                print(agrm.get('number') + " reason: inn:{0}, pn:{1}, sn:{2} ".format(agrm.get('inn'),agrm.get('pn'),agrm.get('sn')), end='')
                ret = cur.callproc('SORM_PROC', (agrm.get('agrm_id'), agrm.get('state'), agrm.get('date'), agrm.get('number'),
                                                 agrm.get('uid'), agrm.get('type'), agrm.get('sole_proprietor'), agrm.get('inn'), agrm.get('pn'), agrm.get('sn'), 0))
                print('add' if ret.get('@_SORM_PROC_arg11') == 2 else 'drop')
                db.commit()
            elif agrm.get('state') == 2 and agrm.get('contractState') == 0: 
                print(agrm.get('number') + " close: ", end='')
                ret = cur.callproc('SORM_PROC', (agrm.get('agrm_id'), agrm.get('state'), agrm.get('date'), agrm.get('number'),
                                                 agrm.get('uid'), agrm.get('type'), agrm.get('sole_proprietor'), agrm.get('inn'), agrm.get('pn'), agrm.get('sn'), 1))
                print('add' if ret.get('@_SORM_PROC_arg11') == 2 else 'drop')
                if ret.get('@_SORM_PROC_arg11') == 2:
                    cur.execute(UPD_CONTRACT, {'contractId': agrm.get('agrm_id')}) 
                    print(agrm.get('number') + " add to report with action=2")
                db.commit()
            else: 
                print(agrm.get('number') + " skip")

    report_full(db)


def report_full(db):
    ''' report full '''

    with cursor(db) as cur:

        cur.execute(QUERY_FULL)
        print(">>>> query full contracts [{0}]".format(cur.rowcount))

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
                    out['UNSTRUCT_NAME'] = ANON_NAME.sub(
                        r'\1*', row.get('name'))  # fix!!!
                    # out['BIRTH_DATE'] = row.get('birthdate')
                    out['BIRTH_DATE'] = '1990-01-01'  # delete
                    out['IDENT_CARD_TYPE_ID'] = row.get('doc_type')
                    out['IDENT_CARD_TYPE'] = 0  # structed
                    out['IDENT_CARD_SERIAL'] = ANON_NUM.sub(
                        '0', row.get('pass_sernum'))  # fix!!!
                    out['IDENT_CARD_NUMBER'] = ANON_NUM.sub(
                        '0', row.get('pass_no'))  # fix!!!
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
        # store batch info
        print("ABONENT: {0} [{1}]".format(filename, cur.rowcount))
        cur.execute("INSERT INTO sorm_batch (batch_name, file_name, file_rec_count) VALUES (%s, %s, %s)", ('abonent', filename, cur.rowcount))
        db.commit()
