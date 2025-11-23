import logging
import pandas as pd
from datetime import datetime, timedelta
from IPOM_Dags.tickting_utils import TicketingManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - Stage %(stage)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ticketing_tasks")

def get_log_dict(airflow_ti, details, action = None):
    LOG_PAYLOAD = {
                "ipom_id": details.get("_id", "Not Found"),
                "task_id": getattr(airflow_ti, "task_id", "Not Found"),
                "run_id": getattr(airflow_ti, "run_id", "Not Found"),
                "try_number": getattr(airflow_ti, "try_number", "Not Found"),
                "ticketing_action": action,
                "count": None,
                "response_status": None,
                "response_message": None,
                "exception": None
                }
    
    return LOG_PAYLOAD

def enrich_dataframe(df, details):
    try:
        logger.info("Populating dataframe with required fields", extra={'stage': '9'})
        df['organization'] = details['organization']
        df['team'] = details['dtTeam']
        df['sub_team'] = details.get('dtSubTeam') or None
        df['subject'] = details['subject']
        df['description'] = details['description']
        df['requester_name'] = details['kpiOwnerName']
        df['requester_email'] = details['kpiOwnerEmail']
        # df['requester_name'] = 'Khadija Yasin'
        # df['requester_email'] = 'khadija.yasin@ascend.com.sa'
        
    except Exception as err:
        logger.error(f"Data population failed due to {err}", extra={'stage': '9'})
        raise
    else:
        return df

def validate_uniques(df):
    """Ensure only one org/team/sub_team/requester exists."""
    for col in ["requester_email", "organization", "team", "sub_team"]:
        unique_vals = df[col].unique()
        if len(unique_vals) != 1:
            logger.error(f"Expected one {col}, got: {unique_vals}", extra={"stage": "11"})
            raise ValueError(f"Expected one {col}, got: {unique_vals}")
    return (
        df["requester_email"].iloc[0],
        df["organization"].iloc[0],
        df["team"].iloc[0],
        df["sub_team"].iloc[0],
    )

def adjust_to_sunday(dt):
    # If Friday → add 2 days
    if dt.weekday() == 4:
        return dt + timedelta(days=2)
    # If Saturday → add 1 day
    elif dt.weekday() == 5:
        return dt + timedelta(days=1)
    else:
        return dt

def open_tickets(ipom_obj, token, details, rule_obj, db_df, tickets_data, airflow_ti = None):
    status = "opened"
    
    condition_series = [ipom_obj.build_condition(db_df, cond) for cond in rule_obj.get('conditions', [])]
    final_condition = ipom_obj.combine_conditions(condition_series, rule_obj.get('conditionLogic'))
    logger.info(f"Records before applying rule conditions: {len(db_df)}", extra={'stage': '8'})

    filtered_df = db_df[final_condition]
    logger.info(f"Records after applying rule conditions: {len(filtered_df)}", extra={'stage': '8'})
    
    if filtered_df.empty:
        logger.warning("No tickets to open. Exiting open_tickets early.", extra={'stage': '8'})
        return

    default_priority = "medium"
    logger.info(f"Setting default priority for all rows as: {default_priority}", extra={'stage': '9'})
    filtered_df['priority'] = default_priority
    
    for priority in details['priorityManagementDs']:
        priority_name = priority.get("priority", "")
        logger.info(f"Processing priority: {priority_name}", extra={'stage': '9'})

        condition_series = [ipom_obj.build_condition(filtered_df, cond) for cond in priority.get('conditions', [])]
        final_condition = ipom_obj.combine_conditions(condition_series, priority.get('conditionLogic'))
        logger.info(f"Records before applying priority conditions: {len(filtered_df)}", extra={'stage': '9'})

        matched_rows = final_condition.sum()
        logger.info(f"Records after applying priority conditions: {matched_rows}", extra={'stage': '9'})

        filtered_df.loc[final_condition, 'priority'] = priority_name
        
    new_dataframe = filtered_df.copy()

    default_due_hours = "48"
    print("Getting Due Hours")
    priority_to_due = {p["priority"].lower(): p.get("dueDate", default_due_hours) for p in details['priorityManagement']}
    
    # Map due hours from priority
    new_dataframe["due_hours"] = new_dataframe["priority"].str.lower().map(priority_to_due)
    
    # Fill any unmapped or missing values with default
    new_dataframe["due_hours"] = new_dataframe["due_hours"].fillna(default_due_hours)

    new_dataframe["duedate"] = new_dataframe["due_hours"].apply(
        lambda h: (adjust_to_sunday(datetime.now() + timedelta(hours=int(h)) - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")) # subtracting 3 hours due to utc time
        if pd.notna(h) else None
    )

    df_to_create = enrich_dataframe(new_dataframe, details)
    requester, org, team, sub_team = validate_uniques(df_to_create)

    unique_col_name = next((item["column"] for item in details["dynamicFieldsUpdateTicket"] if item.get("uuid") is True),None).lower()
    print("Unique Column From DB", unique_col_name)

    unique_uid_ticketing = next((item["name"] for item in details["dynamicFieldsUpdateTicket"] if item.get("uuid") is True),None)
    print("Unique Column From Ticketing System", unique_uid_ticketing)

    unique_dynamic_field = f"dynamicFields.{unique_uid_ticketing}"
    print("Unique Dynamic Field From Ticketing System", unique_uid_ticketing)

    urls = ipom_obj.fetch_ticketing_urls(token)
    ticket_mgr = TicketingManager(urls, details)
    ticket_mgr.login_ticketing(requester)
    ticket_mgr.astrum_url = ipom_obj.base_url

    # req_columns = ['priority', 'duedate', 'organization', 'team', 'sub_team',
    #                 'subject', 'description', 'requester_name', 'requester_email']
    # dynamic_fields_to_keep = [field['column'].lower() for field in ticket_mgr.dynamic_fields]
    # keep_columns = req_columns + dynamic_fields_to_keep
    # df_to_create = df_to_create[[col for col in keep_columns if col in df_to_create.columns]]

    print("Converting Ticketing Data To DataFrame")
    if tickets_data:
        ticketing_df = pd.json_normalize(tickets_data)
        
        print("Removing all closed tickets")
        ticketing_df = ticketing_df[ticketing_df['status_id'] != 'closed']
        
        if unique_dynamic_field in ticketing_df.columns:
            print("Converting Ticketing UID To str type")
            ticketing_df[unique_uid_ticketing] = ticketing_df[unique_dynamic_field].astype(str)
        else:
            print(f"'{unique_dynamic_field}' not found in ticketing data — filling with NA")
            ticketing_df[unique_uid_ticketing] = pd.NA

    else:
        ticketing_df = pd.DataFrame(columns=[unique_uid_ticketing, unique_dynamic_field])

    print(f"Converting {unique_col_name} To str type")
    df_to_create[unique_col_name] = df_to_create[unique_col_name].astype(str)

    print("Finding Unique Rows In Table By Comapring Ticketing Data")
    new_tickets = df_to_create[~df_to_create[unique_col_name].isin(ticketing_df[unique_uid_ticketing])]
    tickets = ticket_mgr.prepare_tickets_to_create(new_tickets)

    chunk_size = 200
    for i in range(0, len(tickets), chunk_size):
        logger.info(f"Processing tickets chunk: {i} to {i + chunk_size}", extra={'stage': '14'})
        payload = tickets[i:i+chunk_size]
        log_dict = get_log_dict(airflow_ti, details, status)
        ticket_mgr.create_ticket_golang(org, payload, log_dict)

def close_tickets(ipom_obj, token, details, rule_obj, db_df, ticket_df, airflow_ti = None):
    status = "closed"
    
    if ticket_df:
        condition_series = [ipom_obj.build_condition(db_df, cond) for cond in rule_obj.get('conditions', [])]
        final_condition = ipom_obj.combine_conditions(condition_series, rule_obj.get('conditionLogic'))
        logger.info(f"Records before applying rule conditions: {len(db_df)}", extra={'stage': '8'})

        filtered_df = db_df[final_condition]
        logger.info(f"Records after applying rule conditions: {len(filtered_df)}", extra={'stage': '8'})
        
        if filtered_df.empty:
            logger.warning("No data to close tickets. Exiting close_tickets early.", extra={'stage': '8'})
            return

        df_to_create = enrich_dataframe(filtered_df, details)
        requester, org, team, sub_team = validate_uniques(df_to_create)

        unique_col_name = next((item["column"] for item in details["dynamicFieldsUpdateTicket"] if item.get("uuid") is True),None).lower()
        print("Unique Column From DB", unique_col_name)

        unique_uid_ticketing = next((item["name"] for item in details["dynamicFieldsUpdateTicket"] if item.get("uuid") is True),None)
        print("Unique Column From Ticketing System", unique_uid_ticketing)

        unique_dynamic_field = f"dynamicFields.{unique_uid_ticketing}"
        print("Unique Dynamic Field From Ticketing System", unique_uid_ticketing)

        urls = ipom_obj.fetch_ticketing_urls(token)
        ticket_mgr = TicketingManager(urls, details)
        ticket_mgr.astrum_url = ipom_obj.base_url

        print("Converting Ticketing Data To DataFrame")
        ticketing_df = pd.json_normalize(ticket_df)
        
        if unique_dynamic_field in ticketing_df.columns:
            print("Converting Ticketing UID To str type")
            ticketing_df[unique_uid_ticketing] = ticketing_df[unique_dynamic_field].astype(str)
        else:
            print(f"'{unique_dynamic_field}' not found in ticketing data — filling with NA")
            ticketing_df[unique_uid_ticketing] = pd.NA

        print(f"Coverting {unique_col_name} To str type")
        df_to_create[unique_col_name] = df_to_create[unique_col_name].astype(str)
        
        print("Removing all closed tickets")
        ticketing_df = ticketing_df[ticketing_df['status_id'] != 'closed']

        print("Matching Rows With Table And Ticketing Data")
        matching_tickets = ticketing_df[ticketing_df[unique_uid_ticketing].isin(df_to_create[unique_col_name])]
        matching_tickets_updated = pd.merge(matching_tickets, df_to_create, left_on=unique_uid_ticketing, right_on=unique_col_name)

        solved_tickets = matching_tickets_updated[matching_tickets_updated['status_id'] == 'solved']
        
        if not solved_tickets.empty:
            print("Closing Solved Tickets")
            log_dict = get_log_dict(airflow_ti, details, status)
            ticket_mgr.status_change_golang(status, solved_tickets, log_dict)
        
        closed_tickets = matching_tickets_updated[matching_tickets_updated['status_id'] != 'solved']
        
        if not closed_tickets.empty:
            print("Solving Tickets Before Closing Them")
            log_dict = get_log_dict(airflow_ti, details, "solved")
            ticket_mgr.status_change_golang("solved", closed_tickets, log_dict)
            print("Closing Solved Tickets")
            log_dict = get_log_dict(airflow_ti, details, status)
            ticket_mgr.status_change_golang(status, closed_tickets, log_dict)
            
        else:
            print(f"No tickets to be {status.lower()}")
    
    else:
        print(f"No tickets available to be {status.lower()}")

def reopen_tickets(ipom_obj, token, details, rule_obj, db_df, ticket_df, airflow_ti = None):
    status = "reopened"
    
    if ticket_df:
        condition_series = [ipom_obj.build_condition(db_df, cond) for cond in rule_obj.get('conditions', [])]
        final_condition = ipom_obj.combine_conditions(condition_series, rule_obj.get('conditionLogic'))
        logger.info(f"Records before applying rule conditions: {len(db_df)}", extra={'stage': '8'})

        filtered_df = db_df[final_condition]
        logger.info(f"Records after applying rule conditions: {len(filtered_df)}", extra={'stage': '8'})
        
        if filtered_df.empty:
            logger.warning("No data to reopen tickets. Exiting reopen_tickets early.", extra={'stage': '8'})
            return

        df_to_create = enrich_dataframe(filtered_df, details)
        requester, org, team, sub_team = validate_uniques(df_to_create)

        unique_col_name = next((item["column"] for item in details["dynamicFieldsUpdateTicket"] if item.get("uuid") is True),None).lower()
        print("Unique Column From DB", unique_col_name)

        unique_uid_ticketing = next((item["name"] for item in details["dynamicFieldsUpdateTicket"] if item.get("uuid") is True),None)
        print("Unique Column From Ticketing System", unique_uid_ticketing)

        unique_dynamic_field = f"dynamicFields.{unique_uid_ticketing}"
        print("Unique Dynamic Field From Ticketing System", unique_uid_ticketing)

        urls = ipom_obj.fetch_ticketing_urls(token)
        ticket_mgr = TicketingManager(urls, details)
        ticket_mgr.astrum_url = ipom_obj.base_url

        print("Converting Ticketing Data To DataFrame")
        ticketing_df = pd.json_normalize(ticket_df)
        
        print("Keeping solved tickets")
        ticketing_df = ticketing_df[ticketing_df['status_id'] == 'solved']
        
        if unique_dynamic_field in ticketing_df.columns:
            print("Converting Ticketing UID To str type")
            ticketing_df[unique_uid_ticketing] = ticketing_df[unique_dynamic_field].astype(str)
        else:
            print(f"'{unique_dynamic_field}' not found in ticketing data — filling with NA")
            ticketing_df[unique_uid_ticketing] = pd.NA

        print(f"Coverting {unique_col_name} To str type")
        df_to_create[unique_col_name] = df_to_create[unique_col_name].astype(str)

        print("Matching Rows With Table And Ticketing Data")
        matching_tickets = ticketing_df[ticketing_df[unique_uid_ticketing].isin(df_to_create[unique_col_name])]
        matching_tickets_updated = pd.merge(matching_tickets, df_to_create, left_on=unique_uid_ticketing, right_on=unique_col_name)

        if not matching_tickets_updated.empty:
            print(f"Reopening solved tickets...")
            log_dict = get_log_dict(airflow_ti, details, status)
            ticket_mgr.status_change_golang(status, matching_tickets_updated, log_dict)
        
        else:
            print(f"No tickets to be {status.lower()}")
    
    else:
        print(f"No tickets available to be {status.lower()}")

def solve_tickets(ipom_obj, token, details, rule_obj, db_df, ticket_df, airflow_ti = None):
    status = "solved"
    
    if ticket_df:
        condition_series = [ipom_obj.build_condition(db_df, cond) for cond in rule_obj.get('conditions', [])]
        final_condition = ipom_obj.combine_conditions(condition_series, rule_obj.get('conditionLogic'))
        logger.info(f"Records before applying rule conditions: {len(db_df)}", extra={'stage': '8'})

        filtered_df = db_df[final_condition]
        logger.info(f"Records after applying rule conditions: {len(filtered_df)}", extra={'stage': '8'})
        
        if filtered_df.empty:
            logger.warning("No data to solve tickets. Exiting solve_tickets early.", extra={'stage': '8'})
            return

        df_to_create = enrich_dataframe(filtered_df, details)
        requester, org, team, sub_team = validate_uniques(df_to_create)

        unique_col_name = next((item["column"] for item in details["dynamicFieldsUpdateTicket"] if item.get("uuid") is True),None).lower()
        print("Unique Column From DB", unique_col_name)

        unique_uid_ticketing = next((item["name"] for item in details["dynamicFieldsUpdateTicket"] if item.get("uuid") is True),None)
        print("Unique Column From Ticketing System", unique_uid_ticketing)

        unique_dynamic_field = f"dynamicFields.{unique_uid_ticketing}"
        print("Unique Dynamic Field From Ticketing System", unique_uid_ticketing)

        urls = ipom_obj.fetch_ticketing_urls(token)
        ticket_mgr = TicketingManager(urls, details)
        ticket_mgr.astrum_url = ipom_obj.base_url

        print("Converting Ticketing Data To DataFrame")
        ticketing_df = pd.json_normalize(ticket_df)
        
        print("Filtering All Tickets Except Closed and Solved Ones")
        ticketing_df = ticketing_df[~ticketing_df['status_id'].isin(['closed', 'solved'])]
        
        if unique_dynamic_field in ticketing_df.columns:
            print("Converting Ticketing UID To str type")
            ticketing_df[unique_uid_ticketing] = ticketing_df[unique_dynamic_field].astype(str)
        else:
            print(f"'{unique_dynamic_field}' not found in ticketing data — filling with NA")
            ticketing_df[unique_uid_ticketing] = pd.NA

        print(f"Coverting {unique_col_name} To str type")
        df_to_create[unique_col_name] = df_to_create[unique_col_name].astype(str)

        print("Matching Rows With Table And Ticketing Data")
        matching_tickets = ticketing_df[ticketing_df[unique_uid_ticketing].isin(df_to_create[unique_col_name])]
        matching_tickets_updated = pd.merge(matching_tickets, df_to_create, left_on=unique_uid_ticketing, right_on=unique_col_name)
        
        if not matching_tickets_updated.empty:
            print(f"Solving opened or reopened tickets...")
            log_dict = get_log_dict(airflow_ti, details, status)
            ticket_mgr.status_change_golang(status, matching_tickets_updated, log_dict)
        else:
            print(f"No tickets to be {status.lower()}")
    
    else:
        print(f"No tickets available to be {status.lower()}")