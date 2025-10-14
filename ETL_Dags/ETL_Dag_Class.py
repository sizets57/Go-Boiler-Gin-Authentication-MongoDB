import logging

logger = logging.getLogger()


class MasterDag:
    def __init__(self):
        import yaml
        import os

        CONFIG_PATH = os.path.join(
            os.getenv("AIRFLOW_HOME", "/opt/airflow"), f"dags/ETL_Dags/config.yml"
        )
        # CONFIG_PATH = r"C:\Users\NishanSalman(Ascend)\Downloads\ascend_astrom_lite\Salman_Astrom_ETL\config.yml"

        with open(CONFIG_PATH, "r") as file:
            config = yaml.safe_load(file)
            self.config = config

        self.logger = {
            "source_schema": None,
            "source_database": None,
            "destination_database": None,
            "source_table": None,
            "destination_table": None,
            "timestamp": None,
            "source_count": 0,
            "destination_count": 0,
            "total_updates": 0,
            "total_writes": 0,
        }

    def GetConfig(self):
        return self.config

    def get_connection_dict(self, df, prefix):
        select_type = df["selectType"][0]

        if select_type == "single":

            connObj = {
                "type": df[f'{prefix}Connection.type'][0].lower(),
                "user": df[f'{prefix}Connection.userName'][0],
                "pass": df[f'{prefix}Connection.password'][0],
                "host": df[f'{prefix}Connection.hostName'][0],
                "port": int(df[f'{prefix}Connection.port'][0]),
                "table": df[f'{prefix}TableName'][0]
            }
            if f'{prefix}Connection.schema' in df.columns:
                connObj['db'] = df[f'{prefix}Connection.schema'][0]
            else:
                connObj['db'] = df[f'{prefix}Connection.databaseName'][0]
            
            return connObj


        elif select_type == "multiple":
            try:
                return {
                    "type": df[f"{prefix}Connection.type"][0].lower(),
                    "user": df[f"{prefix}Connection.userName"][0],
                    "pass": df[f"{prefix}Connection.password"][0],
                    "host": df[f"{prefix}Connection.hostName"][0],
                    "port": int(df[f"{prefix}Connection.port"][0]),
                    "db": df[f"{prefix}Connection.databaseName"][0],
                    "table": df[f"{prefix}TableNames"][0],
                }

            except KeyError as err:
                return {
                    "type": df[f"{prefix}Connection.type"][0].lower(),
                    "user": df[f"{prefix}Connection.userName"][0],
                    "pass": df[f"{prefix}Connection.password"][0],
                    "host": df[f"{prefix}Connection.hostName"][0],
                    "port": int(df[f"{prefix}Connection.port"][0]),
                    "db": df[f"{prefix}Connection.databaseName"][0],
                }

    # def OracleConnection(self, username, password, hostname, port, service=None):
    #     from sqlalchemy import create_engine
    #     from sqlalchemy.pool import NullPool

    #     try:
    #         service_part = f"/?service_name={service}" if service else "/FREEPDB1"
    #         engine_str = f"oracle://{username}:{password}@{hostname}:{port}{service_part}"

    #         logger.info("Attempting Oracle Connection...")
    #         engine = create_engine(engine_str, poolclass=NullPool, arraysize=1000)

    #         with engine.connect():
    #             logger.info("Oracle Connection Established Successfully.")
    #         return engine

    #     except Exception as e:
    #         logger.error(f"Failed To Connect To Oracle DB: {str(e)}", exc_info=True)
    #         return None

    def OracleConnection(self, username, password, hostname, port, service):
        import sys
        import oracledb
        from sqlalchemy.pool import NullPool
        from sqlalchemy.engine import create_engine

        try:
            dialect = "oracle"
            oracledb.version = "8.3.0"
            sys.modules["cx_Oracle"] = oracledb

            # engine_str = dialect+'://'+username+':'+password+'@'+hostname+':'+port+'/?service_name='+service
            engine_str = f"{dialect}://{username}:{password}@{hostname}:{str(port)}/?service_name={service}"
            # engine_str = dialect + '://' + username + ':' + password + '@' + hostname + ':' + str(port) + '/?service_name=' + service
            # engine_str = f"oracle+oracledb://{username}:{password}@{hostname}:{port}/?service_name={service}"
            engine = create_engine(engine_str, poolclass=NullPool, arraysize=1000)
            return engine

        except Exception as e:
            logger.error(f"Failed To Connect To Oracle DB: {str(e)}", exc_info=True)
            return None

    def SQLConnection(self, serverName, dbName, user, pwd):
        from sqlalchemy import create_engine

        try:
            odbc_str = (
                f"Driver={{ODBC Driver 18 for SQL Server}};"
                f"Server={serverName};"
                f"Database={dbName};"
                f"UID={user};"
                f"PWD={pwd};"
                "Encrypt=no;"
                "TrustServerCertificate=yes;"
            )
            connection_uri = f"mssql+pyodbc:///?odbc_connect={odbc_str}"
            logger.info("Attempting SQL Server Connection...")

            engine = create_engine(
                connection_uri,
                future=True,
                isolation_level="READ UNCOMMITTED",
                fast_executemany=True,
                pool_size=10,
                max_overflow=20,
                pool_timeout=400,
                pool_recycle=800,
                pool_pre_ping=True,
            )

            with engine.connect():
                logger.info("SQL Server Connection Established Successfully.")
            return engine

        except Exception as e:
            logger.error(f"Failed To Connect To SQL Server: {str(e)}", exc_info=True)
            return None

    def get_db_engine(self, db_type, user, password, host, port, db_name):
        if db_type.lower() == "oracle":
            return self.OracleConnection(user, password, host, port, db_name)
        elif db_type.lower() == "mssql":
            return self.SQLConnection(f"{host},{port}", db_name, user, password)
        else:
            raise ValueError(f"Unsupported DB type: {db_type}")

    def fetch_query(self, table, db_type, user=None):
        if db_type == "oracle":
            schema = user.upper()
            table = table.upper()
            return f'SELECT * FROM {schema}."{table}"'
        return f"SELECT * FROM {table}"

    def check_table_exist(self, dst, sql_engine):
        import pandas as pd

        if dst["type"] == "oracle":
            check_table_query = f"SELECT owner, table_name FROM ALL_TABLES WHERE OWNER = '{dst["user"].upper()}' AND TABLE_NAME = '{dst["table"].upper()}'"

        elif dst["type"] == "mssql":
            check_table_query = f"SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{dst["table"]}'"

        tables = pd.read_sql_query(check_table_query, sql_engine)
        table_exists = not tables.empty  # If DataFrame is not empty, table exists

        if table_exists:
            logger.info(f"Table {dst['table']} Exists.")
        else:
            logger.warning(f"Table {dst['table']} Does Not Exist.")

        return table_exists

    def get_datatype(self, type, cols, table, sql_engine):
        import pandas as pd

        if type == "oracle":
            check_col_query = f"""
                SELECT 
                OWNER,
                TABLE_NAME,
                COLUMN_NAME as column_name, 
                DATA_TYPE as data_type, 
                DATA_LENGTH, 
                DATA_PRECISION, 
                DATA_SCALE, 
                NULLABLE
            FROM 
                ALL_TAB_COLUMNS
                where COLUMN_NAME IN {"(" + ", ".join(f"'{col.upper()}'" for col in cols) + ")"}
                and TABLE_NAME = '{table.upper()}'
                """

        elif type == "mssql":
            check_col_query = f"""
                SELECT 
                    TABLE_CATALOG AS OWNER,
                    TABLE_NAME,
                    COLUMN_NAME as column_name, 
                    DATA_TYPE as data_type, 
                    CHARACTER_MAXIMUM_LENGTH AS DATA_LENGTH, 
                    NUMERIC_PRECISION AS DATA_PRECISION, 
                    NUMERIC_SCALE AS DATA_SCALE, 
                    IS_NULLABLE AS NULLABLE
                FROM 
                    INFORMATION_SCHEMA.COLUMNS
                WHERE 
                    COLUMN_NAME IN ({", ".join(f"'{col.upper()}'" for col in cols)})
                    AND TABLE_NAME = '{table.upper()}'
                """

        tables = pd.read_sql_query(check_col_query, sql_engine)
        return tables

    # def get_max_date(self, key_column, dst, sql_engine):
    #     import pandas as pd

    #     if dst['type'] == 'oracle':
    #         max_date_query = f'''
    #                 SELECT MAX({key_column[0]}) AS max_{key_column[0]},
    #                 MAX({key_column[1]}) AS max_{key_column[1]}
    #                 FROM {dst['table']}
    #                 '''

    #     elif dst['type'] == 'mssql':
    #         max_date_query = f'''
    #                 SELECT MAX({key_column[0]}) AS max_{key_column[0]},
    #                 MAX({key_column[1]}) AS max_{key_column[1]}
    #                 FROM {dst['table']}
    #                 '''

    #     max_date = pd.read_sql_query(max_date_query, sql_engine)
    #     return max_date

    # def get_updated_data(self, key_column, src, max_timestamps, data_type_df, sql_engine):
    #     import pandas as pd

    #     data_type = data_type_df['data_type'].unique()

    #     if src['type'] == 'oracle':
    #         created_at_str = max_timestamps[0].strftime('%Y-%m-%d %H:%M:%S.%f')
    #         updated_at_str = max_timestamps[1].strftime('%Y-%m-%d %H:%M:%S.%f')
    #         updated_data_query = f'''
    #                             SELECT *
    #                             FROM {src['user']}.{src['table']}
    #                             WHERE CAST({key_column[0]} AS {data_type[0]}) > TO_TIMESTAMP('{created_at_str}', 'YYYY-MM-DD HH24:MI:SS.FF6')
    #                             OR CAST({key_column[1]} AS {data_type[0]}) > TO_TIMESTAMP('{updated_at_str}', 'YYYY-MM-DD HH24:MI:SS.FF6')
    #                             '''

    #     elif src['type'] == 'mssql':
    #         created_at_str = max_timestamps[0].strftime('%Y-%m-%d %H:%M:%S.%f')
    #         updated_at_str = max_timestamps[1].strftime('%Y-%m-%d %H:%M:%S.%f')
    #         updated_data_query = f'''
    #                             SELECT *
    #                             FROM {src['table']}
    #                             WHERE {key_column[0]} > '{created_at_str}'
    #                             OR {key_column[1]} > '{updated_at_str}'
    #                             '''
    #         updated_data_query2 = f'''
    #                             SELECT *
    #                             FROM {src['table']}
    #                             WHERE {key_column[0]} > '{created_at_str[:-3]}'
    #                             OR {key_column[1]} > '{updated_at_str[:-3]}'
    #                             '''
    #     try:
    #         updated_data = pd.read_sql_query(updated_data_query, sql_engine)

    #     except Exception as err:
    #         print("Error: ", err)
    #         print(updated_data_query2)
    #         updated_data = pd.read_sql_query(updated_data_query2, sql_engine)

    #     return updated_data

    def get_max_date(self, key_column, dst, sql_engine):
        import pandas as pd

        select_clauses = [f"MAX({key_column[0]}) AS max_{key_column[0]}"]

        if len(key_column) > 1:
            select_clauses.append(f"MAX({key_column[1]}) AS max_{key_column[1]}")

        max_date_query = f"""
            SELECT {', '.join(select_clauses)}
            FROM {dst['table']}
        """

        max_date = pd.read_sql_query(max_date_query, sql_engine)
        return max_date

    # def get_updated_data(self, key_column, src, max_timestamps, data_type_df, sql_engine):
    #     import pandas as pd

    #     data_type = data_type_df['data_type'].unique()
    #     created_at_str = max_timestamps[0].strftime('%Y-%m-%d %H:%M:%S.%f')

    #     if src['type'] == 'oracle':
    #         query_conditions = [
    #             f"CAST({key_column[0]} AS {data_type[0]}) > TO_TIMESTAMP('{created_at_str}', 'YYYY-MM-DD HH24:MI:SS.FF6')"
    #         ]

    #         if len(key_column) > 1 and max_timestamps[1] is not None:
    #             updated_at_str = max_timestamps[1].strftime('%Y-%m-%d %H:%M:%S.%f')
    #             query_conditions.append(
    #                 f"CAST({key_column[1]} AS {data_type[0]}) > TO_TIMESTAMP('{updated_at_str}', 'YYYY-MM-DD HH24:MI:SS.FF6')"
    #             )

    #         updated_data_query = f"""
    #             SELECT *
    #             FROM {src['user']}.{src['table']}
    #             WHERE {' OR '.join(query_conditions)}
    #         """

    #     elif src['type'] == 'mssql':
    #         query_conditions = [
    #             f"{key_column[0]} > '{created_at_str}'"
    #         ]

    #         updated_at_str = None
    #         if len(key_column) > 1 and max_timestamps[1] is not None:
    #             updated_at_str = max_timestamps[1].strftime('%Y-%m-%d %H:%M:%S.%f')
    #             query_conditions.append(
    #                 f"{key_column[1]} > '{updated_at_str}'"
    #             )

    #         updated_data_query = f"""
    #             SELECT *
    #             FROM {src['table']}
    #             WHERE {' OR '.join(query_conditions)}
    #         """

    #         # Fallback with trimmed microseconds (to 3 digits)
    #         updated_data_query2 = f"""
    #             SELECT *
    #             FROM {src['table']}
    #             WHERE {' OR '.join([
    #                 cond.replace(created_at_str, created_at_str[:-3]) if updated_at_str is None
    #                 else cond.replace(created_at_str, created_at_str[:-3]).replace(updated_at_str, updated_at_str[:-3])
    #                 for cond in query_conditions
    #             ])}
    #         """

    #     try:
    #         updated_data = pd.read_sql_query(updated_data_query, sql_engine)
    #     except Exception as err:
    #         print("Error: ", err)
    #         if src['type'] == 'mssql':  # Only MSSQL has fallback
    #             print(updated_data_query2)
    #             updated_data = pd.read_sql_query(updated_data_query2, sql_engine)
    #         else:
    #             raise

    #     return updated_data

    def get_updated_data(
        self, key_column, src, max_timestamps, data_type_df, sql_engine
    ):
        import pandas as pd
        from datetime import datetime, timedelta

        data_type = data_type_df["data_type"].unique()
        ts_format = "%Y-%m-%d %H:%M:%S.%f"

        # Parse max_timestamps safely (string -> datetime, else keep as datetime)
        created_at = (
            datetime.strptime(max_timestamps[0], ts_format)
            if isinstance(max_timestamps[0], str)
            else max_timestamps[0]
        )
        created_at = created_at - timedelta(days=1)  # Apply 1-day buffer
        created_at_str = created_at.strftime(ts_format)

        updated_at = None
        updated_at_str = None
        if len(max_timestamps) > 1 and max_timestamps[1] is not None:
            updated_at = (
                datetime.strptime(max_timestamps[1], ts_format)
                if isinstance(max_timestamps[1], str)
                else max_timestamps[1]
            )
            updated_at = updated_at - timedelta(days=1)  # Apply 1-day buffer
            updated_at_str = updated_at.strftime(ts_format)

        if src["type"] == "oracle":
            query_conditions = [
                f"CAST({key_column[0]} AS {data_type[0]}) > TO_TIMESTAMP('{created_at_str}', 'YYYY-MM-DD HH24:MI:SS.FF6')"
            ]

            if len(key_column) > 1 and updated_at_str is not None:
                query_conditions.append(
                    f"CAST({key_column[1]} AS {data_type[0]}) > TO_TIMESTAMP('{updated_at_str}', 'YYYY-MM-DD HH24:MI:SS.FF6')"
                )

            updated_data_query = f"""
                SELECT *
                FROM {src['user']}.{src['table']}
                WHERE {' OR '.join(query_conditions)}
            """

        elif src["type"] == "mssql":
            query_conditions = [f"{key_column[0]} > '{created_at_str}'"]

            if len(key_column) > 1 and updated_at_str is not None:
                query_conditions.append(f"{key_column[1]} > '{updated_at_str}'")

            updated_data_query = f"""
                SELECT *
                FROM {src['table']}
                WHERE {' OR '.join(query_conditions)}
            """

            # Fallback with trimmed microseconds (to 3 digits)
            updated_data_query2 = f"""
                SELECT *
                FROM {src['table']}
                WHERE {' OR '.join([
                    cond.replace(created_at_str, created_at_str[:-3]) if updated_at_str is None
                    else cond.replace(created_at_str, created_at_str[:-3]).replace(updated_at_str, updated_at_str[:-3])
                    for cond in query_conditions
                ])}
            """

        try:
            updated_data = pd.read_sql_query(updated_data_query, sql_engine)
        except Exception as err:
            print("Error: ", err)
            if src["type"] == "mssql":  # Only MSSQL has fallback
                print(updated_data_query2)
                updated_data = pd.read_sql_query(updated_data_query2, sql_engine)
            else:
                raise

        return updated_data

    def delete_outdated_rows(self, key_column, dst, data_set, data_type_df, sql_engine):
        from sqlalchemy import text

        formatted_list = [f"'{ts}'" for ts in data_set]
        created_at_list = "(\n  " + ",\n  ".join(formatted_list) + "\n)"
        # data_type = data_type_df['data_type'].unique()

        if dst["type"] == "oracle":
            delete_query = f"DELETE FROM {dst['table']} WHERE TO_CHAR({key_column}) IN {created_at_list}"
            # formatted_list = [f"TO_TIMESTAMP('{ts.strftime('%Y-%m-%d %H:%M:%S.%f')}', 'YYYY-MM-DD HH24:MI:SS.FF6')" for ts in data_set]
            # created_at_list = '(\n  ' + ',\n  '.join(formatted_list) + '\n)'
            # delete_query = f"DELETE FROM {dst['table']} WHERE CAST({key_column[0]} AS {data_type[0]}) IN {created_at_list}"

        elif dst["type"] == "mssql":
            delete_query = (
                f"DELETE FROM {dst['table']} WHERE {key_column} IN {created_at_list}"
            )
            # formatted_list = [f"CAST('{ts.strftime('%Y-%m-%d %H:%M:%S.%f')}' AS {data_type[0]})" for ts in data_set]
            # created_at_list = '(\n  ' + ',\n  '.join(formatted_list) + '\n)'
            # delete_query = f"DELETE FROM {dst['table']} WHERE CAST({key_column[0]} AS {data_type[0]}) IN {created_at_list}"

        with sql_engine.begin() as conn:
            result = conn.execute(text(delete_query))
            logger.info(
                f" {result.rowcount} Rows Are Deleted From Destination Table {dst['table']}"
            )

    def multi_check_table_exist(self, type, table_name, sql_engine, owner=None):
        import pandas as pd

        if type == "oracle":
            check_table_query = f"SELECT owner, table_name FROM ALL_TABLES WHERE OWNER = '{owner.upper()}' AND TABLE_NAME = '{table_name.upper()}'"

        elif type == "mssql":
            check_table_query = f"SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"

        tables = pd.read_sql_query(check_table_query, sql_engine)
        table_exists = not tables.empty  # If DataFrame is not empty, table exists

        if table_exists:
            logger.info(f"Table {table_name} Exists.")
        else:
            logger.warning(f"Table {table_name} Does Not Exist.")

        return table_exists
