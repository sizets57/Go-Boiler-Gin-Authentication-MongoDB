from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task, dag
from airflow.sdk import get_current_context
from datetime import datetime, timedelta
# from IPOM_Dags.sql_logger import SQLHandler
from IPOM_Dags.ipom_utils import DigitalIpom
from IPOM_Dags.tickting_utils import TicketingManager
import IPOM_Dags.ticketing_tasks as ticketing_tasks
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - Stage %(stage)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("main_file")

ipom_id = "68a57e12f5335a81c2b695a8"
ipom_obj = DigitalIpom()

@task(show_return_value_in_logs=False)
def get_data_from_db(token, details):
    context = get_current_context()
    ti = context["ti"]
    
    db_conn = ipom_obj.fetch_connection(token, details['connectionId'])
    engine = ipom_obj.calling_get_db_engine(db_conn)
    df = ipom_obj.load_and_prepare_dataframe(engine, details, ti)
    return df

@task(show_return_value_in_logs=False)
def fetch_all_tickets_data(token, details):
    context = get_current_context()
    ti = context["ti"]
    
    urls = ipom_obj.fetch_ticketing_urls(token)
    ticket_mgr = TicketingManager(urls, details)

    logger.info("Fetching tickets data", extra={"stage": "6"})
    data = ticket_mgr.get_tickets_data(ti)
    logger.info("Ticket data fetched", extra={"stage": "6"})

    return data

@task
def opening_tickets(token, details, rule_obj, db_df, ticket_df):
    logger.info(f"Criteria to {rule_obj['ruleName']} tickets is {rule_obj['description']}", extra={'stage': '7'})
    logger.info(f"Computing conditions to filter data to {rule_obj['ruleName']} tickets", extra={'stage': '7'})
    
    context = get_current_context()
    ti = context["ti"]
    
    ticketing_tasks.open_tickets(ipom_obj, token, details, rule_obj, db_df, ticket_df, ti)

@task
def closing_tickets(token, details, rule_obj, db_df, ticket_df):
    logger.info(f"Criteria to {rule_obj['ruleName']} tickets is {rule_obj['description']}", extra={'stage': '7'})
    logger.info(f"Computing conditions to filter data to {rule_obj['ruleName']} tickets", extra={'stage': '7'})

    context = get_current_context()
    ti = context["ti"]
    ticketing_tasks.close_tickets(ipom_obj, token, details, rule_obj, db_df, ticket_df, ti)

@task
def reopening_tickets(token, details, rule_obj, db_df, ticket_df):
    logger.info(f"Criteria to {rule_obj['ruleName']} tickets is {rule_obj['description']}", extra={'stage': '7'})
    logger.info(f"Computing conditions to filter data to {rule_obj['ruleName']} tickets", extra={'stage': '7'})

    context = get_current_context()
    ti = context["ti"]
    ticketing_tasks.reopen_tickets(ipom_obj, token, details, rule_obj, db_df, ticket_df, ti)

@task
def solving_tickets(token, details, rule_obj, db_df, ticket_df):
    logger.info(f"Criteria to {rule_obj['ruleName']} tickets is {rule_obj['description']}", extra={'stage': '7'})
    logger.info(f"Computing conditions to filter data to {rule_obj['ruleName']} tickets", extra={'stage': '7'})

    context = get_current_context()
    ti = context["ti"]
    ticketing_tasks.solve_tickets(ipom_obj, token, details, rule_obj, db_df, ticket_df, ti)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 28),
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

@dag (
    dag_id='$kpiName',
    default_args = default_args,
    max_active_tasks=1, 
    catchup=False,
    schedule='@pipelineFrequency'
    )

def ipom_processor():
    token = ipom_obj.fetch_token()
    details = ipom_obj.fetch_ipom_details(token, ipom_id)
    
    ticket_data_1 = None
    ticket_data_2 = None

    db_data = get_data_from_db(token, details)
    ticket_data_1 = fetch_all_tickets_data.override(task_id = "fetch_existing_tickets")(token, details)
    
    last_task = ticket_data_1
    db_data >> last_task
    
    rules = details['rules']
    rules = ipom_obj.sort_rules(rules)
    
    tasks = {
        "close": closing_tickets,
        "open": opening_tickets,
        "reopen": reopening_tickets,
        "solve": solving_tickets,
        }

    for rule in rules:
        rule_name = rule.get("ruleName", "").lower()
        print("RULE NAME:", repr(rule_name))
        
        if rule_name == 'close':
            current_task = tasks[rule_name].override(task_id=f"{rule_name}_tickets",
                                            execution_timeout=timedelta(minutes=30))\
                                            (token = token, details = details, rule_obj = rule, db_df = db_data, ticket_df = ticket_data_1)
            
            last_task >> current_task
            
            ticket_data_2 = fetch_all_tickets_data.override(task_id = "fetch_existing_tickets_after_closing")(token, details)
            current_task >> ticket_data_2
            last_task = ticket_data_2

        else:
            ticket_data = ticket_data_2 if ticket_data_2 else ticket_data_1
            
            current_task = tasks[rule_name].override(task_id=f"{rule_name}_tickets",
                                            execution_timeout=timedelta(minutes=30))\
                                            (token = token, details = details, rule_obj = rule, db_df = db_data, ticket_df = ticket_data)
                                            
            last_task >> current_task
            last_task = current_task

ipom_dag = ipom_processor()