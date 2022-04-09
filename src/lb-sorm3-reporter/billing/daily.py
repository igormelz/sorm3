from utils import cursor, cursorDef
import logging

QUERY_AGRM = '''
SELECT a.uid, a.agrm_id, a.number, a.state, a.date, a.last_mod_date,
    s.contractState, s.hasReason, s.reason,
    u.`type`, u.sole_proprietor, CHAR_LENGTH(u.inn) inn, CHAR_LENGTH(u.pass_no) pn, CHAR_LENGTH(u.pass_sernum) sn
FROM agreements a
    LEFT OUTER JOIN sorm s ON (a.agrm_id = s.contractId)
    INNER JOIN accounts u ON(a.uid = u.uid AND u.uid > 20)
WHERE 
    a.last_mod_date > (SELECT max(batch_time) FROM sorm_batch WHERE batch_name ='abonent')
	AND (s.contractState IS NULL OR s.hasReason = 1 OR s.contractState != a.state)
'''

DEL_CLOSE_CONTRACTS = 'DELETE FROM sorm WHERE record_action = 1 and contractState = 2'
DEL_MARKED_REC = 'DELETE FROM sorm WHERE record_action = 2'
UPD_PROCESSED_CONTRACTS = 'UPDATE sorm SET record_action = 4 WHERE record_action = 1'
UPD_CONTRACT = 'UPDATE sorm SET record_action = 2 WHERE contractId = %(contractId)s'
INS_CONTRACT = 'INSERT INTO sorm (contractId, contractState, contractDate, contract, abonentId, attach, detach) VALUES (%s,%s,%s,%s,%s,%s,%s)'
INS_WAIT = 'INSERT INTO sorm (contractId, contractState, contractDate, contract, abonentId, reason, hasReason, record_action) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
UPD_REASON_OK = 'UPDATE sorm SET record_action = 1, hasReason = 0, reason = NULL, attach = %(attach)s, detach = %(detach)s WHERE contractId = %(contractId)s'
UPD_REASON = 'UPDATE sorm SET reason = %(reason)s, hasReason = 1, record_action = 3 WHERE contractId = %(contractId)s'


def sorm_proc(cursor, contract):
    return cursor.callproc('SORM_PROC', (
        contract.get('agrm_id'),
        contract.get('state'),
        contract.get('date'),
        contract.get('number'),
        contract.get('uid'),
        contract.get('type'),
        contract.get('sole_proprietor'),
        contract.get('inn'),
        contract.get('pn'),
        contract.get('sn'),
        0, 0, 0, 0
    ))


def pre_process(db):
    ''' report daily '''

    # preprocess
    with cursorDef(db) as cur:
        # do preprocessing
        cur.execute(DEL_CLOSE_CONTRACTS)
        logging.info(f"drop close contracts [{cur.rowcount}]")
        cur.execute(DEL_MARKED_REC)
        logging.info(f"drop removed contracts [{cur.rowcount}]")
        cur.execute(UPD_PROCESSED_CONTRACTS)
        logging.info(f"mark processed contracts [{cur.rowcount}]")

    with cursor(db) as cur:

        # get new and changed contracts
        cur.execute(QUERY_AGRM)
        contracts = cur.fetchall()
        logging.info(f"select contracts to process [{cur.rowcount}]")

        # process contracts
        for agrm in contracts:

            if agrm.get('contractState') == None:
                # get new contract details
                ret = sorm_proc(cur, agrm)
                if ret.get('@_SORM_PROC_arg14') == None and ret.get('@_SORM_PROC_arg13') == None:
                    cur.execute(INS_CONTRACT, (agrm.get('agrm_id'), agrm.get('state'), agrm.get('date'), agrm.get(
                        'number'), agrm.get('uid'), ret.get('@_SORM_PROC_arg11'), ret.get('@_SORM_PROC_arg12')))
                    logging.info("contract %s new: %s" %
                                 (agrm.get('number'), 'add to report'))

                elif ret.get('@_SORM_PROC_arg14') == None and ret.get('@_SORM_PROC_arg13') != None:
                    # insert waiting
                    cur.execute(INS_WAIT, (agrm.get('agrm_id'), agrm.get('state'), agrm.get(
                        'date'), agrm.get('number'), agrm.get('uid'), ret.get('@_SORM_PROC_arg13'), 1, 3))
                    logging.info('contract %s new: %s: reason:%s' % (
                        agrm.get('number'), 'set wait', ret.get('@_SORM_PROC_arg13')))
                else:
                    logging.info('contract %s new: %s' %
                                 (agrm.get('number'), 'skip with no login yet'))

            elif agrm.get('hasReason') == 1:
                # get contract details for change reason
                ret = sorm_proc(cur, agrm)
                if ret.get('@_SORM_PROC_arg14') == None and ret.get('@_SORM_PROC_arg13') == None:
                    # update to report
                    cur.execute(UPD_REASON_OK, {'contractId': agrm.get('agrm_id'), 'attach': ret.get(
                        '@_SORM_PROC_arg11'), 'detach': ret.get('@_SORM_PROC_arg12')})
                    logging.info('contract %s reason: %s' % (
                        agrm.get('number'), 'clear, record add to report'))

                elif ret.get('@_SORM_PROC_arg14') == None and ret.get('@_SORM_PROC_arg13') != None and agrm.get('reason') != ret.get('@_SORM_PROC_arg13'):
                    cur.execute(UPD_REASON, {'contractId': agrm.get(
                        'agrm_id'), 'reason': ret.get('@_SORM_PROC_arg13')})
                    logging.info('contract %s reason: %s (%s -> %s)' % (agrm.get('number'),
                                 'change', agrm.get('reason'), ret.get('@_SORM_PROC_arg13')))

                else:
                    logging.info('contract %s reason: %s' %
                                    (agrm.get('number'), 'same. record skip'))

            elif agrm.get('state') == 2 and agrm.get('contractState') == 0:
                # contract close
                ret = sorm_proc(cur, agrm)
                if ret.get('@_SORM_PROC_arg14') == None and ret.get('@_SORM_PROC_arg13') == None:
                    # mark record to print
                    cur.execute(UPD_CONTRACT, {
                                'contractId': agrm.get('agrm_id')})
                    # insert close contract
                    cur.execute(INS_CONTRACT, (agrm.get('agrm_id'), agrm.get('state'), agrm.get('date'), agrm.get(
                        'number'), agrm.get('uid'), ret.get('@_SORM_PROC_arg11'), ret.get('@_SORM_PROC_arg12')))
                    logging.info('contract %s close: %s' %
                                 (agrm.get('number'), 'add'))

                else:
                    logging.info('contract %s close: %s' %
                                    (agrm.get('number'), 'skip by has reason'))

            else:
                logging.warning("contract %s skip" % agrm.get('number'))
    db.commit()
