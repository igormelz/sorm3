from reports.utils import writer, cursor, e164
from datetime import date, timedelta

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
select = '''SELECT 
    a.number,
    a.date,
    a.archive,
    a.last_mod_date,
    u.uid,
    u.type,
    u.name,
    u.birthdate,
    u.sole_proprietor,
    u.pass_sernum,
    u.pass_no,
    u.pass_issuedate,
    u.pass_issueplace,
    u.bank_name, 
    u.inn, 
    u.settl, 
    u.gen_dir_u, 
    u.phone,
    v.acc_ondate, 
    v.acc_offdate,
    d.address 
    FROM agreements a 
    INNER JOIN accounts u USING(uid) 
    INNER JOIN vgroups v USING(agrm_id) 
    INNER JOIN accounts_addr d ON(u.uid = d.uid AND d.type = 0)
'''


def query(full):
    if full == True:
        # return all until today
        return select + " WHERE a.last_mod_date < '" + str(date.today()) + "'"
    else:
        # return yesterday
        return select + " WHERE DATE(a.last_mod_date) = '" + str(date.today() - timedelta(days=1)) + "'"


def isActive(row):
    if(int(row.get('archive')) == 0):
        return '2024-12-31'
    else:
        return row.get('dt')


def report(db, full=False, debug=False):
    print("BILLING: ABONENT ....... ", end='')
    with cursor(db) as cur:
        if debug:
            print(query(full))
        cur.execute(query(full))
        with writer(FORMAT, FIELDS) as csvout:
            for row in cur:
                # if debug:
                #     print(row)
                # map out record
                out = {
                    'REGION_ID': 1,
                    'CONTRACT_DATE': row.get('date'),
                    'CONTRACT': row.get('number'),
                    'ACCOUNT': row.get('number')
                }
                if(row.get('type') == 2):
                    out['ABONENT_TYPE'] = "42"
                    out['NAME_INFO_TYPE'] = 1  # unstructed name
                    out['UNSTRUCT_NAME'] = row.get('name')
                    out['BIRTH_DATE'] = row.get('birthdate')
                    out['IDENT_CARD_TYPE_ID'] = 1  # passport
                    out['IDENT_CARD_TYPE'] = 0  # structed
                    out['IDENT_CARD_SERIAL'] = row.get('pass_sernum')
                    out['IDENT_CARD_NUMBER'] = row.get('pass_no')
                    # out['IDENT_CARD_DESCRIPTION'] = "Выдан:" + \
                    #     str(row.get('pass_issuedate'))+" Место:" + \
                    #     str(row.get('pass_issueplace'))
                else:
                    out['ABONENT_TYPE'] = "42"
                csvout.writerow(out)
    print("FULL" if full else "INCR")
