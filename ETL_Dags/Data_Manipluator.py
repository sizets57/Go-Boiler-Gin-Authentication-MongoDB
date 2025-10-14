from ETL_Dags.ETL_Dag_Class import MasterDag
from sqlalchemy import create_engine
import sys

dag_instance = MasterDag()
config = dag_instance.GetConfig()

import logging

logger = logging.getLogger()


def loginToSystem():
    import requests
    import pandas as pd

    baseUrl = config["Backend"]["baseUrl"]
    url = baseUrl + "/login"

    payload = {
        "email": config["Backend"]["email"],
        "password": config["Backend"]["password"],
    }

    headers = {}
    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code != 200:
        raise ValueError("Login to System Failed", response.text)

    df = pd.json_normalize(response.json())
    token = df["data.token"][0]
    headers = {"Authorization": "Bearer " + token}

    return headers


def log_writer(df):
    db_host = config["MYSQL_LOG"]["host"]
    db_port = config["MYSQL_LOG"]["port"]
    db_user = config["MYSQL_LOG"]["username"]
    db_password = config["MYSQL_LOG"]["password"]
    db_name = config["MYSQL_LOG"]["db_name"]

    connection_string = (
        f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )
    engine = create_engine(connection_string)
    connection = engine.connect()

    logger.info("Writing logs into Database")
    with engine.connect() as connection:
        df.to_sql(
            name="astrum_etl_logs", con=connection, if_exists="append", index=False
        )


def estimate_df_size(df):
    # Size in memory
    mem_bytes = df.memory_usage(deep=True).sum()
    # Size if serialized to CSV (closer to network transfer)
    csv_bytes = sys.getsizeof(df.to_csv(index=False).encode("utf-8"))
    return mem_bytes, csv_bytes


def TransferData(df, dag_instance):
    import pandas as pd
    from datetime import datetime
    from sqlalchemy.dialects.oracle import TIMESTAMP, NVARCHAR as oracle_navchar
    from sqlalchemy.dialects.mssql import DATETIME2, NVARCHAR as mssql_navchar

    fetch_time = datetime.utcnow()

    elt_log = {
        "pipeline_id": None,
        "src_table": None,
        "src_database": None,
        "src_type": None,
        "number_of_src_tables": None,
        "src_count": None,
        "dst_table": None,
        "dst_database": None,
        "dst_type": None,
        "number_of_dst_tables": None,
        "dst_count": None,
        "dst_max_created_at": None,
        "dst_max_updated_at": None,
        "deleted_records": None,
        "primary_key": None,
        "intergartion_type": None,
        "mode_type": None,
        "fetch_time": None,
        "memory_usage": None,
        "network_usage": None,
    }

    src = dag_instance.get_connection_dict(df, "source")
    dst = dag_instance.get_connection_dict(df, "destination")

    src_type, dst_type = src["type"].lower(), dst["type"].lower()
    mode_type = df["selectType"][0].lower()
    schema = dst["user"] if dst_type == "oracle" else None

    elt_log["pipeline_id"] = df["_id"][0]
    elt_log["src_type"] = src_type
    elt_log["dst_type"] = dst_type
    elt_log["fetch_time"] = fetch_time
    elt_log["mode_type"] = mode_type

    if mode_type == "single":
        logger.info(
            f"Source: {src_type}::{src['table']} | Destination: {dst_type}::{dst['table']}"
        )

        elt_log["src_table"] = src["table"]
        elt_log["src_database"] = src["db"]
        elt_log["dst_table"] = dst["table"]
        elt_log["dst_database"] = dst["db"]
        elt_log["number_of_src_tables"] = 1
        elt_log["number_of_dst_tables"] = 1

    elif mode_type == "multiple":
        logger.info(
            f"Source: {src_type}::{src['table']} | Destination: {dst_type}::{src['table']}"
        )

    source_engine = dag_instance.get_db_engine(
        src_type, src["user"], src["pass"], src["host"], src["port"], src["db"]
    )
    dest_engine = dag_instance.get_db_engine(
        dst_type, dst["user"], dst["pass"], dst["host"], dst["port"], dst["db"]
    )

    if not source_engine or not dest_engine:
        logger.error("One or both Database Connections Failed.")
        return

    if dst_type == "oracle":
        dtype = {"created_at": TIMESTAMP(3), "updated_at": TIMESTAMP(3)}
        text_type = oracle_navchar(511)

    else:  # mssql
        dtype = {"created_at": DATETIME2(6), "updated_at": DATETIME2(6)}
        text_type = mssql_navchar(511)

    if mode_type == "single":
        existance = dag_instance.check_table_exist(dst, dest_engine)

        src_query = dag_instance.fetch_query(src["table"], src_type, src["user"])
        print("Source Query:", src_query)
        src_df = pd.read_sql_query(src_query, source_engine)
        elt_log["src_count"] = len(src_df)

        integration_type = df["destinationIntegrationType"][0].lower()
        print("Integration Type:", integration_type)
        elt_log["intergartion_type"] = integration_type

        if integration_type == "replace":
            logger.info("Replacing Data In Destination Table.")
            schema = dst["user"].upper() if dst_type == "oracle" else None

            txt_cols = src_df.select_dtypes(include=["object"]).columns
            dtype.update({col: text_type for col in txt_cols if col not in dtype})

            mem_bytes, csv_bytes = estimate_df_size(src_df)
            memory_usage = f"{mem_bytes / (1024 * 1024):.2f} MB"
            network_usage = f"{csv_bytes / (1024 * 1024):.2f} MB"

            elt_log["memory_usage"] = memory_usage
            elt_log["network_usage"] = network_usage
            logger.info(
                f"Transfer size → Memory: {memory_usage} | CSV: {network_usage}"
            )

            # src_df.to_sql(dst['table'], dest_engine, schema=schema, if_exists='replace', index=False, chunksize=10000)
            src_df.to_sql(
                dst["table"],
                dest_engine,
                schema=schema,
                if_exists="replace",
                index=False,
                chunksize=10000,
                dtype=dtype,
            )

            elt_log["dst_count"] = len(src_df)
            logger.info(
                f"{len(src_df)} rows inserted in table {dst['user'].upper()}.\"{dst['table']}\""
            )

        elif integration_type == "append":
            if "updated" in df.columns:
                key_column = [df["created"][0], df["updated"][0]]
            else:
                key_column = [df["created"][0]]
            # key_column = ['created_at', 'updated_at']
            print("key_column:", key_column)

            primary_key = df["primaryId"][0]
            # primary_key = ['created_at']
            print("primary_key:", primary_key)

            if existance:
                source_dtype_df = dag_instance.get_datatype(
                    src_type, key_column, src["table"], source_engine
                )
                dest_dtype_df = dag_instance.get_datatype(
                    dst_type, key_column, dst["table"], dest_engine
                )

                max_date = dag_instance.get_max_date(key_column, dst, dest_engine)
                max_timestamp = max_date.iloc[0].tolist()

                elt_log["dst_max_created_at"] = str(max_timestamp[0])
                elt_log["dst_max_updated_at"] = str(max_timestamp[1])

                logger.info(f"Maximum Timestamp For {key_column} Are {max_timestamp}")
                changes_df = dag_instance.get_updated_data(
                    key_column, src, max_timestamp, source_dtype_df, source_engine
                )
                elt_log["src_count"] = len(changes_df)

                if not changes_df.empty:
                    logger.info(
                        f"Updated Records Found In Source Table {src['table'].upper()} Are {len(changes_df)}"
                    )
                    txt_cols = changes_df.select_dtypes(include=["object"]).columns
                    dtype.update(
                        {col: text_type for col in txt_cols if col not in dtype}
                    )

                    mem_bytes, csv_bytes = estimate_df_size(changes_df)
                    memory_usage = f"{mem_bytes / (1024 * 1024):.2f} MB"
                    network_usage = f"{csv_bytes / (1024 * 1024):.2f} MB"

                    elt_log["memory_usage"] = memory_usage
                    elt_log["network_usage"] = network_usage
                    logger.info(
                        f"Transfer size → Memory: {memory_usage} | CSV: {network_usage}"
                    )

                    changes_df.to_sql(
                        dst["table"],
                        dest_engine,
                        schema=schema,
                        if_exists="append",
                        index=False,
                        chunksize=10000,
                        dtype=dtype,
                    )
                    elt_log["dst_count"] = len(changes_df)
                    logger.info(f"Appended {len(changes_df)} Records.")
                else:
                    elt_log["dst_count"] = len(changes_df)
                    logger.info("No New or Updated Records Found.")

            else:
                src_query = dag_instance.fetch_query(
                    src["table"], src_type, src["user"]
                )
                print("Source Query:", src_query)
                src_df = pd.read_sql_query(src_query, source_engine)
                elt_log["src_count"] = len(src_df)

                txt_cols = src_df.select_dtypes(include=["object"]).columns
                dtype.update({col: text_type for col in txt_cols if col not in dtype})

                mem_bytes, csv_bytes = estimate_df_size(src_df)
                memory_usage = f"{mem_bytes / (1024 * 1024):.2f} MB"
                network_usage = f"{csv_bytes / (1024 * 1024):.2f} MB"

                elt_log["memory_usage"] = memory_usage
                elt_log["network_usage"] = network_usage
                logger.info(
                    f"Transfer size → Memory: {memory_usage} | CSV: {network_usage}"
                )

                src_df.to_sql(
                    dst["table"],
                    dest_engine,
                    schema=schema,
                    if_exists="replace",
                    index=False,
                    chunksize=10000,
                    dtype=dtype,
                )
                elt_log["dst_count"] = len(src_df)
                logger.info(
                    f"Table {dst['user'].upper()}.\"{dst['table']}\" Created And Data Inserted."
                )

        elif integration_type == "sync":
            key_column = [df["created"][0], df["updated"][0]]
            # key_column = ['created_at', 'updated_at']
            print("key_column:", key_column)

            primary_key = df["primaryId"][0]
            print("primary_key:", primary_key)

            if existance:
                source_dtype_df = dag_instance.get_datatype(
                    src_type, key_column, src["table"], source_engine
                )
                dest_dtype_df = dag_instance.get_datatype(
                    dst_type, key_column, dst["table"], dest_engine
                )

                max_date = dag_instance.get_max_date(key_column, dst, dest_engine)
                max_timestamp = max_date.iloc[0].tolist()

                elt_log["dst_max_created_at"] = str(max_timestamp[0])
                elt_log["dst_max_updated_at"] = str(max_timestamp[1])

                logger.info(f"Maximum Timestamp For {key_column} Are {max_timestamp}")
                changes_df = dag_instance.get_updated_data(
                    key_column, src, max_timestamp, source_dtype_df, source_engine
                )
                elt_log["src_count"] = len(changes_df)

                if not changes_df.empty:
                    logger.info(
                        f"Updated Records Found In Source Table {src['table'].upper()} are {len(changes_df)}"
                    )
                    del_records = dag_instance.delete_outdated_rows(
                        primary_key,
                        dst,
                        changes_df[primary_key].tolist(),
                        dest_dtype_df,
                        dest_engine,
                    )
                    elt_log["deleted_records"] = len(del_records)

                    txt_cols = changes_df.select_dtypes(include=["object"]).columns
                    dtype.update(
                        {col: text_type for col in txt_cols if col not in dtype}
                    )

                    mem_bytes, csv_bytes = estimate_df_size(changes_df)
                    memory_usage = f"{mem_bytes / (1024 * 1024):.2f} MB"
                    network_usage = f"{csv_bytes / (1024 * 1024):.2f} MB"

                    elt_log["memory_usage"] = memory_usage
                    elt_log["network_usage"] = network_usage
                    logger.info(
                        f"Transfer size → Memory: {memory_usage} | CSV: {network_usage}"
                    )

                    changes_df.to_sql(
                        dst["table"],
                        dest_engine,
                        schema=schema,
                        if_exists="append",
                        index=False,
                        chunksize=10000,
                        dtype=dtype,
                    )
                    elt_log["dst_count"] = len(changes_df)
                    logger.info(f"Upserted {len(changes_df)} Updated/New Records.")

                else:
                    elt_log["deleted_records"] = 0
                    elt_log["dst_count"] = len(changes_df)
                    logger.info("No New or Updated Records Found.")

            else:
                src_query = dag_instance.fetch_query(
                    src["table"], src_type, src["user"]
                )
                print("Source Query:", src_query)
                src_df = pd.read_sql_query(src_query, source_engine)
                elt_log["src_count"] = len(src_df)

                txt_cols = src_df.select_dtypes(include=["object"]).columns
                dtype.update({col: text_type for col in txt_cols if col not in dtype})

                mem_bytes, csv_bytes = estimate_df_size(src_df)
                memory_usage = f"{mem_bytes / (1024 * 1024):.2f} MB"
                network_usage = f"{csv_bytes / (1024 * 1024):.2f} MB"

                elt_log["memory_usage"] = memory_usage
                elt_log["network_usage"] = network_usage
                logger.info(
                    f"Transfer size → Memory: {memory_usage} | CSV: {network_usage}"
                )

                src_df.to_sql(
                    dst["table"],
                    dest_engine,
                    schema=schema,
                    if_exists="replace",
                    index=False,
                    chunksize=10000,
                    dtype=dtype,
                )
                elt_log["dst_count"] = len(src_df)
                logger.info(
                    f"Table {dst['user'].upper()}.\"{dst['table']}\" Created And Data Inserted."
                )

        else:
            logger.warning(
                f"Unsupported Destination Integration Type: {integration_type}"
            )
            # return

        logs_df = pd.DataFrame([elt_log])

    elif mode_type == "multiple":
        # src_df = pd.DataFrame()
        multiple_lst = []

        for tbl in src["table"]:
            elt_log = {
                "pipeline_id": None,
                "src_table": None,
                "src_database": None,
                "src_type": None,
                "number_of_src_tables": None,
                "src_count": None,
                "dst_table": None,
                "dst_database": None,
                "dst_type": None,
                "number_of_dst_tables": None,
                "dst_count": None,
                "dst_max_created_at": None,
                "dst_max_updated_at": None,
                "deleted_records": None,
                "primary_key": None,
                "intergartion_type": None,
                "mode_type": None,
                "fetch_time": None,
                "memory_usage": None,
                "network_usage": None,
            }

            existance = dag_instance.multi_check_table_exist(
                src_type, tbl, source_engine, src["user"]
            )

            elt_log["pipeline_id"] = df["_id"][0]
            elt_log["intergartion_type"] = "multiple"
            elt_log["mode_type"] = "replace"
            elt_log["fetch_time"] = fetch_time
            elt_log["src_table"] = tbl
            elt_log["dst_table"] = tbl
            elt_log["number_of_src_tables"] = len(src["table"])
            elt_log["number_of_dst_tables"] = len(src["table"])
            elt_log["src_database"] = src["db"]
            elt_log["dst_database"] = dst["db"]

            if existance:
                src_query = dag_instance.fetch_query(tbl.lower(), src_type, src["user"])
                print("Source Query:", src_query)

                src_df = pd.read_sql_query(src_query, source_engine)
                elt_log["src_count"] = len(src_df)
                logger.info("Replacing Data In Destination Table.")

                txt_cols = src_df.select_dtypes(include=["object"]).columns
                dtype.update({col: text_type for col in txt_cols if col not in dtype})

                mem_bytes, csv_bytes = estimate_df_size(src_df)
                memory_usage = f"{mem_bytes / (1024 * 1024):.2f} MB"
                network_usage = f"{csv_bytes / (1024 * 1024):.2f} MB"

                elt_log["memory_usage"] = memory_usage
                elt_log["network_usage"] = network_usage
                logger.info(
                    f"Transfer size → Memory: {memory_usage} | CSV: {network_usage}"
                )

                src_df.to_sql(
                    tbl,
                    dest_engine,
                    schema=schema,
                    if_exists="replace",
                    index=False,
                    chunksize=10000,
                    dtype=dtype,
                )
                elt_log["dst_count"] = len(src_df)
                logger.info(
                    f"Table {dst['user'].upper()}.\"{tbl}\" Created And Data Inserted."
                )

                multiple_lst.append(elt_log)
                # src_df = pd.concat([src_df, temp_df], ignore_index=True)
            else:
                elt_log["src_count"] = 0
                elt_log["dst_count"] = 0
                continue

        logs_df = pd.DataFrame(elt_log)

    log_writer(logs_df)
    logger.info(
        f"Data {integration_type} To {dst_type}::{dst['table']} Completed Successfully."
    )
    return src_df
