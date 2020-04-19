import datetime
from utils import *
import re


def validate_duplicate_records(environment, src_database, src_schema, src_table, req_id, ora_conn):
    # pdb.set_trace()
    table_check = execute_oracle_df_qry(ora_conn, QUERIES['SELECT_QUERY'].format(src_database, src_table, src_schema,
                                                                                 environment))
    if table_check[table_check.isin([req_id])].stack().empty and table_check.empty:

        execute_oracle_qry(ora_conn, QUERIES['INSERT_QUERY'].format(req_id, src_database, src_table, src_schema,
                                                                    environment))

    elif table_check[table_check.isin([req_id])].stack().empty and not table_check.empty:
        table_id = table_check['table_id'][0]

        execute_oracle_qry(ora_conn, QUERIES['INSERT_QUERY_2'].format(req_id, src_database, src_table, src_schema,
                                                                      environment, table_id))

    elif not table_check[table_check.isin([req_id])].stack().empty and not table_check.empty:
        pass

    execute_oracle_qry(ora_conn, 'COMMIT')
    data = execute_oracle_df_qry(ora_conn,
                                 QUERIES['GROUP_ID_QUERY'].format(req_id, environment, src_database, src_table,
                                                                  src_schema))

    return data


def get_target_data(env, table, conn, sf_env):
    # pdb.set_trace()
    target_db_name = target_table_name = target_schema_name = refresh_date = ''

    target_df = execute_oracle_df_qry(conn, QUERIES['TARGET_QUERY'].format(env, env, table))

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


def corona_tables(env, schema, src_table, conn):
    # pdb.set_trace()
    if schema == 'REPLICDB' and src_table.startswith('S_'):
        if src_table.startswith('S_MT_'):
            suggested_table = src_table.replace('S_MT_', 'MT_')
        else:
            table = src_table.replace('S_', 'N_')
            query = """select * from {}.edw_job_streams where source_schema = '{}' and target_table_name = '{}'""".format(
                env, schema, table)
            n_df = execute_oracle_df_qry(conn, query)
            if n_df.empty:
                table = src_table.replace('S_', 'R_')
                query = """select * from {}.edw_job_streams where source_schema = '{}' and target_table_name = '{}'""".format(
                    env, schema, table)
                mt_df = execute_oracle_df_qry(conn, query)
                if mt_df.empty:
                    suggested_table = src_table.replace('S_', 'N_')
                else:
                    suggested_table = table
            else:
                suggested_table = table
    elif src_table.startswith('PV_'):
        table = src_table.replace('PV_', 'N_')
        suggested_table = table
    elif src_table.startswith('BV_'):
        table = src_table.replace('BV_', 'N_')
        suggested_table = table
    # elif schema == 'REFADM' and src_table.startswith('CG1_'):
    #     suggested_table = src_table
    else:
        suggested_table = src_table

    return suggested_table


def domain_mapping(df, predicted_table, actual_table, env=''):
    # pdb.set_trace()

    l_error = ''
    if df.empty:
        l_error = l_error + 'Mapping does not exist please contact Architects '
        suggested_db = suggested_schema = ''
        suggested_table = predicted_table
    else:
        suggested_db = df['sf_db_name'][0]
        suggested_schema = df['sf_db_schema'][0]
        suggested_table = df['sf_target_table'][0]
        # suggested_db = df['sf_db_name'][0] + env
        #
        # suggested_schema = df['sf_db_schema'][0]
        # if predicted_table == actual_table and not actual_table.startswith('CG1_'):
        #     prefix = df['attribute1'][0]
        #     if prefix is not None:
        #         suggested_table = prefix + '_' + actual_table
        #     else:
        #         suggested_table = actual_table
        # else:
        #     suggested_table = predicted_table

    return suggested_db, suggested_schema, suggested_table, l_error


def object_mapping(data, ora_conn, mapping_conn,request_id):
    # pdb.set_trace()
    for index, row in data.iterrows():
        source_db = row['source_db_name']
        source_schema = row['source_schema_name']
        source_table = row['source_table_name']
        target_env = row['environment']
        if target_env=='DEV':
           domain_env='DV3'
        elif target_env=='STG':
           domain_env='TS3'
        else:
           domain_env='PRD'
        try:
            mapping_df = execute_oracle_df_qry(mapping_conn,QUERIES['MAPPING_QUERY'].format(domain_env, source_db, source_schema,source_table))
            # mapping_df = execute_oracle_df_qry(mapping_conn, QUERIES['MAPPING_QUERY'].format(source_db, source_schema))
            # pdb.set_trace()
        except Exception as err:
            mapping_df = pd.DataFrame()
            if re.search('Please contact SF Data Ingestion Team', str(err)):
                err_msg = "Mapping does not exist please contact Architects"
                upd_qry = """ update ejc.edw_di_request_list set error_msg = '{}' where request_id = '{}' and
                     source_db_name = '{}' and source_table_name = '{}' and source_schema_name = '{}'
                     and environment = '{}'""".format(err_msg, request_id, source_db, source_table, source_schema, target_env)
                execute_oracle_qry(ora_conn, upd_qry)
                execute_oracle_qry(ora_conn, 'COMMIT')

        if target_env == 'DEV':
            suggested_table = corona_tables('ejcdv3', source_schema, source_table, ora_conn)
            suggested_db, suggested_schema, suggested_table, l_error = domain_mapping(mapping_df, suggested_table,
                                                                                      source_table, '_DV3')
            target_db, target_schema, target_table, refresh_date = get_target_data('ejcdv3', suggested_table, ora_conn,
                                                                                   'SF_DEV')

        elif target_env == 'STG':
            suggested_table = corona_tables('ejcts3', source_schema, source_table, ora_conn)
            suggested_db, suggested_schema, suggested_table, l_error = domain_mapping(mapping_df, suggested_table,
                                                                                      source_table, '_TS3')
            target_db, target_schema, target_table, refresh_date = get_target_data('ejcts3', suggested_table, ora_conn,
                                                                                   'SF_STG')

        elif target_env == 'PRD':
            suggested_table = corona_tables('ejc', source_schema, source_table, ora_conn)
            suggested_db, suggested_schema, suggested_table, l_error = domain_mapping(mapping_df, suggested_table,
                                                                                      source_table)
            target_db, target_schema, target_table, refresh_date = get_target_data('ejc', suggested_table, ora_conn,
                                                                                   'SF_PRD')

        return suggested_db, suggested_schema, suggested_table, target_db, target_schema, target_table, refresh_date, l_error


def script(environment, src_db, src_schema, src_table, request_id, application_name, ejcro_conn, eds_conn):
    try:
        import pdb
        # pdb.set_trace()
        records = validate_duplicate_records(environment, src_db, src_schema, src_table, request_id, ejcro_conn)

        sugg_db, sugg_schema, sugg_table, target_db, target_schema, target_table, last_refresh_date, error = object_mapping(
            records, ejcro_conn, ejcro_conn,request_id)

        if isinstance(last_refresh_date, datetime.datetime):
            last_refresh_date = last_refresh_date.strftime('%d-%b-%Y %I:%M:%S %p').upper()
        elif last_refresh_date is None:
            last_refresh_date = ''
        else:
            last_refresh_date = last_refresh_date


        if target_table == '':
            process_req = 'P'
        else:
            process_req = 'C'

        rep_sql="""select DRV_TABLE_NAME  from ( select SUBSTR(table_name,instr(table_name,'_',1)+1 )as DRV_TABLE_NAME from EDW_TABLE_REP where
        DB_INSTANCE_NAME = 'ODSPROD' AND db_schema_name in ( 'OPS$ODSADM', 'GGACML', 'OPS$GGADM') ) rep where DRV_TABLE_NAME like '%{}%'""".format(src_table)

        rep_df=execute_oracle_df_qry(eds_conn,rep_sql)
        if len(rep_df)!=0 and src_db!='TDPROD':
            comments='This  table exists in ODS, pls load it from ODSPROD'
        else:
            comments=''
        execute_oracle_qry(ejcro_conn,
                           QUERIES['UPDATE_QUERY'].format(sugg_db, sugg_schema, sugg_table, error,
                                                          target_db, target_schema, target_table, process_req,
                                                          application_name,comments, request_id, src_db, src_table, src_schema,
                                                          environment))

        execute_oracle_qry(ejcro_conn, 'COMMIT')
        execute_oracle_qry(ejcro_conn,
                           "update ejc.edw_di_request_list set refresh_date = '{}' where request_id = '{}' and source_db_name = '{}' and source_table_name = '{}' and source_schema_name = '{}' and environment = '{}'".format(
                               last_refresh_date, request_id, src_db, src_table, src_schema, environment))

        execute_oracle_qry(ejcro_conn, 'COMMIT')
    except Exception as err:
        print(str(err))


