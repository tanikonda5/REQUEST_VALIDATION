#!/apps/python/install/bin/python

#########################################################################################################################
# di_request_list.py                                                                                                    #
# Script to insert di requests and suggest the target_db, target_schema, target_table                                   #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Siva            Jan-2020                Initial Version                                                 #
# 1.1           Siva            Jan-2020                Added target db,schema,tables logic                             #
# 1.2           Siva            Jan-2020                Added Carona Tables                                             #
#########################################################################################################################

import argparse
import os

from data_validation import script
from utils import *

QUERIES = collect_property_file_contents('config.ini', 'QUERIES')

os.environ['ORACLE_HOME'] = "/usr/cisco/packages/oracle/current"
os.environ[
    'LD_LIBRARY_PATH'] = "/usr/cisco/packages/oracle/current/lib:/usr/cisco/packages/oracle/current/lib:$LD_LIBRARY_PATH"
os.environ['PATH'] = "${ORACLE_HOME}/bin:$PATH"


def check_arg(args=None):
    parser = argparse.ArgumentParser(
        description="Script to validate metadata for each environment "
    )
    parser.add_argument('-r',
                        dest="request_id",
                        help='request_id',
                        required=True)

    input_args = parser.parse_args(args)
    return input_args.request_id.upper()


if __name__ == '__main__':
    req_id = check_arg(sys.argv[1:])

    eds_conn = create_oracle_connection('EDS')
    ejcro_conn = create_oracle_connection('EJCRO')

    req_df = execute_oracle_df_qry(ejcro_conn, QUERIES['SCRIPT_SELECT_QUERY'].format(req_id))
    for i in range(len(req_df)):
        table_id = req_df['table_id'][i]
        env = req_df['environment'][i]
        src_db = req_df['source_db_name'][i]
        src_schema = req_df['source_schema_name'][i]
        src_table = req_df['source_table_name'][i]
        request_id = req_df['request_id'][i]
        if req_df['application_name'][i] is None:
            application_name = ''
        else:
            application_name = req_df['application_name'][i]


        if src_db is None or src_schema is None or src_table is None:
            execute_oracle_qry(ejcro_conn, QUERIES['INACTIVE_QUERY'].format(request_id, table_id))
            execute_oracle_qry(ejcro_conn, 'COMMIT')

        else:

            script(env.strip(), src_db.strip(), src_schema.strip(), src_table.strip(), request_id.strip(),
                   application_name, ejcro_conn, eds_conn)
        print(i)


