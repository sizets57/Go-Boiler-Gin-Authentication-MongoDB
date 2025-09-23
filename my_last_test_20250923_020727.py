from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task, dag
from datetime import datetime, timedelta
from IPOM_Dags.sql_logger import SQLHandler
from IPOM_Dags.ipom_utils import DigitalIpom
from IPOM_Dags.tickting_utils import TicketingManager
import logging

ipom_id = "68d2391f65686ee02e74ba2d"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - Stage %(stage)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("main_file")

ipom_obj = DigitalIpom()


def fetch_token(ipom_obj):
    res = None
    try:
        logger.info("Fetching token to login Astrum-lite", extra={"stage": "2"})
        res = ipom_obj.astrum_login()
        res.raise_for_status()
        content = res.json()
        data = content.get("data")
        token = data.get("token") if data and "token" in data else None
        if not token:
            raise ValueError("Token not found in login response.")
        return token
    except Exception as e:
        logger.error(f"Login failed: {e}", extra={"stage": "2"})
        if res is not None:
            logger.error(f"Response: {res.text}", extra={"stage": "2"})
        raise


def fetch_ipom_details(ipom_obj, token, ipom_id):
    res = None
    try:
        logger.info("Fetching IPOM details", extra={"stage": "3"})
        res = ipom_obj.get_ipom_details(token, ipom_id)
        res.raise_for_status()
        content = res.json()
        data = content.get("data")
        details = data.get("ipom")
        if not details:
            raise ValueError("IPOM details not found.")
        return details
    except Exception as e:
        logger.error(f"Failed to fetch IPOM details: {e}", extra={"stage": "3"})
        if res is not None:
            logger.error(f"Response: {res.text}", extra={"stage": "2"})
        raise


def fetch_ticketing_urls(ipom_obj, token):
    res = None
    try:
        logger.info("Fetching ticketing base urls", extra={"stage": "10"})
        res = ipom_obj.get_ticketing_urls(token)
        res.raise_for_status()
        content = res.json()
        conn = content.get("data")
        return conn
    except Exception as e:
        logger.error(f"Failed to fetch urls: {e}", extra={"stage": "10"})
        if res is not None:
            logger.error(f"Response: {res.text}", extra={"stage": "2"})
        raise


def fetch_connection(ipom_obj, token, connection_id):
    res = None
    try:
        logger.info("Fetching DB connection details", extra={"stage": "4"})
        res = ipom_obj.get_connection_details(token, connection_id)
        res.raise_for_status()
        content = res.json()
        conn = content.get("data")
        return conn
    except Exception as e:
        logger.error(f"Failed to fetch DB connection: {e}", extra={"stage": "4"})
        if res is not None:
            logger.error(f"Response: {res.text}", extra={"stage": "2"})
        raise


def apply_priority_rules(df, rules, ipom):
    import pandas as pd

    if "priority" not in df.columns:
        df["priority"] = pd.NA
    if "duedate" not in df.columns:
        df["duedate"] = pd.NA

    logger.info("Applying priority rules", extra={"stage": "9"})

    now = datetime.now()
    for rule in rules:
        try:
            condition = ipom.fix_condition(rule["threshold"])
            idx = df.query(condition).index
            df.loc[idx, "priority"] = rule["priority"].lower()
            df.loc[idx, "duedate"] = (
                now + timedelta(hours=int(rule["dueDate"]))
            ).strftime("%Y-%m-%d 18:59:00")

        except Exception:
            logger.warning(
                f"Failed applying rule: {rule['threshold']}",
                exc_info=True,
                extra={"stage": "9"},
            )

    fallback_idx = df[df["priority"].isna()].index

    # default priority medium
    df.loc[fallback_idx, "priority"] = "medium"

    # default duedate is 3 days from now
    df.loc[fallback_idx, "duedate"] = (now + timedelta(days=3)).strftime(
        "%Y-%m-%d 18:59:00"
    )

    new_df = df.replace("nan", pd.NA).copy()
    return new_df


def load_and_prepare_dataframe(engine, query):
    import pandas as pd

    try:
        logger.info(f"Extracting data from source table", extra={"stage": "6"})
        df = pd.read_sql(query, engine)
        df.columns = df.columns.str.lower()
    except Exception as err:
        logger.error(f"Data extraction failed: {err}", extra={"stage": "6"})
        raise
    else:
        return df


def enrich_dataframe(df, details):
    try:
        logger.info("Populating dataframe with required fields", extra={"stage": "9"})
        df["organization"] = details["organization"]
        df["team"] = details["dtTeam"]
        df["sub_team"] = details["dtSubTeam"] if "sub_team" in df.columns else None
        df["subject"] = details["subject"]
        df["description"] = details["description"]
        df["requester_name"] = details["kpiOwnerName"]
        df["requester_email"] = details["kpiOwnerEmail"]

    except Exception as err:
        logger.error(f"Data population failed due to {err}", extra={"stage": "9"})
        raise
    else:
        return df


@task
def get_data_from_db(token, details):
    if not any(
        isinstance(handle, SQLHandler) for handle in logging.getLogger().handlers
    ):
        sql_handler = SQLHandler(ipom_id=ipom_id)
        sql_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(message)s")
        sql_handler.setLevel(logging.INFO)

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(sql_handler)

    db_conn = fetch_connection(ipom_obj, token, details["connectionId"])

    db_conn_password = str(ipom_obj.decrypt(db_conn["password"]))
    engine = ipom_obj.get_db_engine(
        str(db_conn["type"]),
        str(db_conn["userName"]),
        db_conn_password,
        str(db_conn["hostName"]),
        str(db_conn["port"]),
        str(db_conn["databaseName"]),
    )

    df = load_and_prepare_dataframe(engine, details.get("query"))

    return df


@task
def opening_tickets(token, details, rule_obj, df):
    if not any(
        isinstance(handle, SQLHandler) for handle in logging.getLogger().handlers
    ):
        sql_handler = SQLHandler(ipom_id=ipom_id)
        sql_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(message)s")
        sql_handler.setLevel(logging.INFO)

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(sql_handler)

    logger.info(
        f"Criteria to {rule_obj['ruleName']} tickets is {rule_obj['description']}",
        extra={"stage": "7"},
    )

    logger.info(
        f"Computing conditions to filter data to {rule_obj['ruleName']} tickets",
        extra={"stage": "7"},
    )
    condition_series = [
        ipom_obj.build_condition(df, cond) for cond in rule_obj.get("conditions", [])
    ]

    final_condition = ipom_obj.combine_conditions(
        condition_series, rule_obj.get("conditionLogic")
    )

    logger.info(f"Records before applying conditions: {len(df)}", extra={"stage": "8"})
    filtered_df = df[final_condition]
    logger.info(
        f"Records after applying conditions: {len(filtered_df)}", extra={"stage": "8"}
    )

    df_to_create = apply_priority_rules(
        filtered_df, details["priorityManagement"], ipom_obj
    )

    if len(df_to_create[df_to_create["priority"].isna()] > 0):
        logger.critical(
            f"Records with no priority: {len(df_to_create[df_to_create['priority'].isna()])}",
            extra={"stage": "9"},
        )
        print(df_to_create)
        df_to_create = df_to_create[df_to_create["priority"].notna()]

    df_to_create = enrich_dataframe(df_to_create, details)

    urls = fetch_ticketing_urls(ipom_obj, token)
    ticket_mgr = TicketingManager(urls, details["dynamicFieldsUpdateTicket"])

    unique_user = df_to_create["requester_email"].unique()
    if len(unique_user) != 1:
        logger.error(
            f"Expected one requester, got: {unique_user}", extra={"stage": "11"}
        )
        raise ValueError(f"Expected one requester, got: {unique_user}")

    unique_org = df_to_create["organization"].unique()
    if len(unique_org) != 1:
        logger.error(
            f"Expected one organization, got: {unique_org}", extra={"stage": "11"}
        )
        raise ValueError(f"Expected one organization, got: {unique_org}")

    org_id = unique_org[0]

    ticket_mgr.login_ticketing(unique_user[0])

    req_columns = [
        "priority",
        "duedate",
        "organization",
        "team",
        "sub_team",
        "subject",
        "description",
        "requester_name",
        "requester_email",
    ]
    dynamic_fields_to_keep = [
        field["column"].lower() for field in ticket_mgr.dynamic_fields
    ]
    keep_columns = req_columns + dynamic_fields_to_keep
    df_to_create = df_to_create[
        [col for col in keep_columns if col in df_to_create.columns]
    ]

    tickets = ticket_mgr.prepare_tickets_to_create(df_to_create)

    chunk_size = 200
    for i in range(0, len(tickets), chunk_size):
        logger.info(
            f"Processing tickets chunk: {i} to {i + chunk_size}", extra={"stage": "14"}
        )
        payload = tickets[i : i + chunk_size]
        ticket_mgr.create_ticket_golang(org_id, payload)


@task
def close_tickets():
    pass


def reopen_tickets():
    pass


def update_tickets():
    pass


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 28),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="$kpiName",
    default_args=default_args,
    max_active_tasks=1,
    catchup=False,
    schedule="@Near to real time",
)
def ipom_processor():
    token = fetch_token(ipom_obj)
    details = fetch_ipom_details(ipom_obj, token, ipom_id)

    data = get_data_from_db(token, details)

    rules = details["rules"]

    previous_task = None
    for rule in rules:
        if rule["ruleName"] == "Open":
            current_task = opening_tickets.override(
                task_id=f"{rule['ruleName']}_Tickets",
                trigger_rule=TriggerRule.ALL_DONE,
                execution_timeout=timedelta(minutes=20),
            )(token=token, details=details, rule_obj=rule, df=data)

        elif rule["ruleName"] == "Close":
            current_task = close_tickets.override(
                task_id=f"{rule['ruleName']}_Tickets",
                trigger_rule=TriggerRule.ALL_DONE,
                execution_timeout=timedelta(minutes=20),
            )()

        elif rule["ruleName"] == "Reopen":
            current_task = reopen_tickets.override(
                task_id=f"{rule['ruleName']}_Tickets",
                trigger_rule=TriggerRule.ALL_DONE,
                execution_timeout=timedelta(minutes=20),
            )()

        elif rule["ruleName"] == "Updated at":
            current_task = update_tickets.override(
                task_id=f"Update_Tickets",
                trigger_rule=TriggerRule.ALL_DONE,
                execution_timeout=timedelta(minutes=20),
            )()

        if previous_task:
            previous_task >> current_task

        previous_task = current_task


ipom_dag = ipom_processor()
