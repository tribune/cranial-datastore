import psycopg2

from cranial.common import logger

log = logger.get()

def _get_redshift_connection(key_path):
    with open(key_path) as file:
        credentials = file.read().strip()
        parts = credentials.split(':')

        return psycopg2.connect(host=parts[0],
                                port=parts[1],
                                dbname=parts[2],
                                user=parts[3],
                                password=parts[4])

def execute_redshift_query(sql_statement, key_path, dry_run=False):
    if dry_run:
        log.info(sql_statement)
        return

    log.debug('Executing SQL:\n{}.'.format(sql_statement))
    try:
        sql_con = _get_redshift_connection(key_path)
        sql_con.set_isolation_level(0)
        log.debug(sql_statement)
        with sql_con.cursor() as c:
            c.execute(sql_statement)

        sql_con.close()

    except psycopg2.Error as e:
        if "VACUUM is running" in e.pgerror:
            log.info("Vacuum is already running elsewhere.  Skipping.")
        else:
            log.info(e.pgcode, e.pgerror)
            raise e