/python/install/bin/python
from data_validation import corona_tables, domain_mapping, get_target_data
import datetime
from utils import *
import sys
import argparse
import os
import re

QUERIES = collect_property_file_contents('config.ini', 'QUERIES')

os.environ['ORACLE_HOME'] = "/usr/cisco/packages/oracle/current"
os.environ['LD_LIBRARY_PATH'] = "/usr/cisco/packages/oracle/current/lib:/usr/cisco/packages/oracle/current/lib" \
                                ":$LD_LIBRARY_PATH "
os.environ['PATH'] = "${ORACLE_HOME}/bin:$PATH"


def check_arg(args=None):
    parser = argparse.ArgumentParser(
        description="Script to validate metadata for each environment "
    )
    parser.add_argument('-r',
                        dest="request_id",
                        help='request_id',
                        required=False)

    input_args = parser.parse_args(args)
    return input_args.request_id


def get_tgt_data(env, table, conn, sf_env, src_db, src_schema, suggested_table):
    # pdb.set_trace()
    target_db_name = target_table_name = target_schema_name = refresh_date = ''

    target_df = execute_oracle_df_qry(conn, QUERIES['TARGET_QUERY'].format(env, env, suggested_table))
    if target_df.empty:
        target_df = execute_oracle_df_qry(conn, QUERIES['TARGET_SRC_QUERY'].format(env, env, table, src_db, src_schema))

    # print (QUERIES['TARGET_SRC_QUERY'].format(env, env, table,src_db,src_schema))

    if not target_df.empty:
        target_db_name = target_df['target_db_name'][0]
        target_schema_name = target_df['target_schema'][0]
        target_table_name = target_df['target_table_name'][0]
        sf_conn = create_sf_connection(sf_env)
        try:
            sf_qry = QUERIES['REFRESH_QUERY'].format(target_db_name, target_schema_name, target_table_name)

            refresh_df = execute_sf_qry(sf_conn, sf_qry)
            if not refresh_df.empty:
                refresh_date = refresh_df['last_refresh_time'][0]
            close_connection(sf_conn)
        except Exception as e:
            close_connection(sf_conn)

    return target_db_name, target_schema_name, target_table_name, refresh_date


if __name__ == '__main__':

    req_id = check_arg(sys.argv[1:])
    eds_conn = create_oracle_connection('EDS')
    ora_conn = create_oracle_connection('EJCRO')
    if req_id is None:
        df = execute_oracle_df_qry(ora_conn, QUERIES['TARGET_CRON_QUERY'])
    else:
        req_id = req_id.upper()
        df = execute_oracle_df_qry(ora_conn, QUERIES['TARGET_CRON_REQ_QUERY'].format(req_id))

    for i in range(len(df)):
        try:
            table_id = df['table_id'][i]
            target_env = df['environment'][i]
            source_db = df['source_db_name'][i]
            source_schema = df['source_schema_name'][i]
            source_table = df['source_table_name'][i]
            request_id = df['request_id'][i]
            application_name = df['application_name'][i]
            if target_env =='DEV':
               dom_env ='DV3'
            elif target_env =='STG':
               dom_env ='TS3'
            else :
               dom_env ='PRD'

            mapping_df = execute_oracle_df_qry(ora_conn,
                                           QUERIES['MAPPING_QUERY'].format(dom_env, source_db, source_schema,
                                                                           source_table))
            # mapping_df = execute_oracle_df_qry(eds_conn, QUERIES['MAPPING_QUERY'].format(source_db, source_schema))
            # pdb.set_trace()
            if target_env == 'DEV':
                suggested_table = corona_tables('ejcdv3', source_schema, source_table, ora_conn)
                suggested_db, suggested_schema, suggested_table, l_error = domain_mapping(mapping_df, suggested_table,
                                                                                          source_table, '_DV3')
                target_db, target_schema, target_table, refresh_date = get_tgt_data('ejcdv3', source_table, ora_conn,
                                                                                    'SF_DEV', source_db, source_schema,
                                                                                    suggested_table)

            elif target_env == 'STG':
                suggested_table = corona_tables('ejcts3', source_schema, source_table, ora_conn)
                suggested_db, suggested_schema, suggested_table, l_error = domain_mapping(mapping_df, suggested_table,
                                                                                          source_table, '_TS3')
                target_db, target_schema, target_table, refresh_date = get_tgt_data('ejcts3', source_table, ora_conn,
                                                                                    'SF_STG', source_db, source_schema,
                                                                                    suggested_table)

            elif target_env == 'PRD':
                suggested_table = corona_tables('ejc', source_schema, source_table, ora_conn)
                suggested_db, suggested_schema, suggested_table, l_error = domain_mapping(mapping_df, suggested_table,
                                                                                          source_table)
                target_db, target_schema, target_table, refresh_date = get_tgt_data('ejc', source_table, ora_conn,
                                                                                    'SF_PRD', source_db, source_schema,
                                                                                    suggested_table)

            if isinstance(refresh_date, datetime.datetime):
                refresh_date = refresh_date.strftime('%d-%b-%Y %I:%M:%S %p').upper()
            if refresh_date is None:
                refresh_date = ''

            if target_table == '':
                process_req = 'P'
            else:
                process_req = 'C'

            rep_sql = """ select DRV_TABLE_NAME  from ( select  SUBSTR(table_name,instr(table_name,'_',1)+1 )as DRV_TABLE_NAME from EDW_TABLE_REP where
                       DB_INSTANCE_NAME = 'ODSPROD' AND db_schema_name in ( 'OPS$ODSADM', 'GGACML', 'OPS$GGADM') ) rep where DRV_TABLE_NAME like '%{}%'""".format(
                source_table)

            rep_df = execute_oracle_df_qry(eds_conn, rep_sql)
            if len(rep_df) != 0 and source_db != 'TDPROD':
                comments = 'This  table exists in ODS, pls load it from ODSPROD'
            else:
                comments = ''

            execute_oracle_qry(ora_conn,
                               QUERIES['UPDATE_QUERY'].format(suggested_db, suggested_schema, suggested_table, l_error,
                                                              target_db, target_schema, target_table, process_req,
                                                              application_name, comments, request_id, source_db,
                                                              source_table,
                                                              source_schema, target_env))

            execute_oracle_qry(ora_conn, 'COMMIT')
            execute_oracle_qry(ora_conn,
                               "update ejc.edw_di_request_list set refresh_date = '{}' where request_id = '{}'   and source_db_name = '{}' and source_table_name = '{}' and source_schema_name = '{}' and environment = '{}'".format(
                                   refresh_date, request_id, source_db, source_table, source_schema, target_env))
            execute_oracle_qry(ora_conn, 'COMMIT')
        except Exception as err:
            print(str(err))
            if re.search('Please contact SF Data Ingestion Team', str(err)):
                err_msg = "Mapping does not exist please contact Architects"
                upd_qry = """ update ejc.edw_di_request_list set error_msg = '{}' where request_id = '{}' and
                     source_db_name = '{}' and source_table_name = '{}' and source_schema_name = '{}'
                     and environment = '{}'""".format(err_msg, request_id, source_db, source_table, source_schema, target_env)
                execute_oracle_qry(ora_conn, upd_qry)
                execute_oracle_qry(ora_conn, 'COMMIT')

