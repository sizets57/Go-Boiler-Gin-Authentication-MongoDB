import requests
import logging
import json, time
import regex as re
from dataclasses import dataclass, asdict
from typing import List, Dict
from MasterDag import MasterDag

dag_instance = MasterDag()
config = dag_instance.GetConfig()


@dataclass
class Ticket:
    subject: str
    content: str
    team_ids: List[str]
    sub_team: List[str]
    priority_id: str
    status_id: str
    due_date: str
    dynamic_fields: List[Dict[str, str]]
    ticketCreateKey: str
    createdPlatform: str

class TicketingManager:
    def __init__(self, urls, details):
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - Stage %(stage)s - %(name)s - %(levelname)s - %(message)s")
        self.logger = logging.getLogger("ticketing_utils")

        self.logger.info("Reading config file for fetching Ticketing System related data...", extra={'stage': '11'})
        self.login_password = config['ENOVI']['password']

        self.dynamic_fields = details['dynamicFieldsUpdateTicket']
        self.larvel_url = urls['baseUrlLaravel']
        self.golang_url = urls['baseUrlGo']
        self.ipom_details = details
        self.user = details['kpiOwnerEmail']
        self.org_id = details['organization']
        
        self.login_ticketing(self.user)

    def login_ticketing(self, email):
        payload = {'email': email, 
                    'password': self.login_password}
        login_url = self.larvel_url.rstrip("/") + "/login?forData=true"
        try:
            response = requests.post(login_url, data=payload)
            self.logger.info(f"Status: {response.status_code}", extra={'stage': '12'})

            response.raise_for_status()

            content = response.json()
            token = content.get("refreshToken")
        
        except Exception as err:
            self.logger.error(f"Response: {response.text}", extra={'stage': '12'})
            self.logger.error(f"Unexpected error during login: {err}", extra={'stage': '12'})
            raise

        else:
            self.header = {"Authorization": f"Bearer {token}"}

    def replace_placeholders_format(self, text, row):
        PLACEHOLDER_PATTERN = r"\[\[(.*?)\]\]"
        PLACEHOLDER_FORMAT = "[[{}]]"
        
        pattern = re.compile(PLACEHOLDER_PATTERN)
        matched = re.findall(pattern, text)
        
        if not matched:
            # print("No placeholders found in text.")
            return text
        
        lower_map = {col.lower(): col for col in row.keys()}

        for key in matched:
            placeholder = PLACEHOLDER_FORMAT.format(key)
            lookup_key = key.strip().lower()
            
            if key in row:
                value = row[key]
                # print(f"Matched '{key}' directly with value '{value}'")
                
            elif lookup_key in lower_map:
                real_col = lower_map[lookup_key]
                value = row[real_col]
                # print(f"Matched '{key}' (case-insensitive) to column '{real_col}' with value '{value}'")
            
            else:
                value = ""
            
            text = text.replace(placeholder, str(value))
        
        return text

    def log_writer(self, log_dict):
        dashboard_api = self.astrum_url + "/dashboard/push?token=GcyaRAtfp2"
        print("Log API:", dashboard_api)
        
        payload = {
            "type": "ipom_logs",
            "payload": log_dict
        }
        
        headers = {'Content-Type': 'application/json'}
        
        MAX_RETRIES = 3
        RETRY_DELAY = 2

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                print(f"Attempt {attempt} of {MAX_RETRIES}...")
                request = requests.post(
                    dashboard_api,
                    data=json.dumps(payload),
                    headers=headers
                )

                status = request.status_code
                print("Log write status:", status)
                print("Log write response:", request.text)

                if 200 <= status < 300:
                    break
                else:
                    print(f"Non-success status ({status}). Retrying in {RETRY_DELAY}s...")
                    time.sleep(RETRY_DELAY)

            except requests.exceptions.RequestException as e:
                print(f"Request failed on attempt {attempt}: {e}")
                if attempt < MAX_RETRIES:
                    print(f"Retrying in {RETRY_DELAY}s...")
                    time.sleep(RETRY_DELAY)
                else:
                    print("All retries failed.")
        
    def prepare_tickets_to_create(self, df):
        tickets = []
        try:
            for ind, row in df.iterrows():
                row_keys = {k.lower(): k for k in row.keys()}
                ticket_subject = self.replace_placeholders_format(row["subject"], row)
                ticket_description = self.replace_placeholders_format(row["description"], row)
                
                ticket = Ticket(
                    subject = ticket_subject,
                    content = ticket_description,
                    team_ids = [row["team"]],
                    sub_team = [row["sub_team"]] if row["sub_team"] is not None else [],
                    priority_id = row["priority"].lower(),
                    status_id = "opened",
                    due_date = row["duedate"],
                    createdPlatform = "astrum-lite",
                    dynamic_fields = [{field['id']: str(row[row_keys[field['column'].lower()]]) if row[row_keys[field['column'].lower()]] is not None else None}
                                    for field in self.dynamic_fields
                                    if field['column'].lower() in row_keys],
                    ticketCreateKey=""
                )
                tickets.append(asdict(ticket))
        except Exception as err:
            self.logger.error(f"Error occured due to: {err}", extra={'stage': '13'})
        else:
            self.logger.info(f"Prepared {len(tickets)} tickets to create.", extra={'stage': '13'})
            return tickets

    def get_tickets_data(self, airflow_ti = None):
        import json
        import requests
        from concurrent.futures import ThreadPoolExecutor, as_completed

        self.login_ticketing(self.user)
        team_id = self.ipom_details['dtTeam']
        sub_team_id = self.ipom_details.get('dtSubTeam')
        org_id = self.org_id

        payload = {
            "sideFilter": {
                "range": {},
                "overdue": False,
                "status_id": [],
                "child_team": [],
                "priority_id": [],
                "parent_teams": [team_id],
                "dynamic_fieldsFilter": [],
                "TicketID": "",
                "myTickets": False,
                "filter": ""
            },
            "role": "Super Admin (RPA)"
        }

        if sub_team_id:
            payload["sideFilter"]["child_team"] = [sub_team_id]

        # Fetch pages
        fetch_pages_golang = self.golang_url.rstrip("/") + f"/tickets/chartData/{org_id}?page=1&perPage=5000"
        print(fetch_pages_golang)
        print("Payload:", payload)

        request_fetch_pages_golang = requests.post(fetch_pages_golang, data=json.dumps(payload), headers=self.header).json()
        print("Details Of Data To Be Fetched:", request_fetch_pages_golang)
        pages_in_fetch_api = request_fetch_pages_golang.get("TotalPage", 0)

        if pages_in_fetch_api == 0:
            print("No Pages To Fetch.")
            return

        def fetch_data(page, session):
            fetch_data_golang = self.golang_url.rstrip("/") + f"/tickets/{org_id}?page={page}&perPage=5000"
            retry_count = 0
            max_retries = 5
            while retry_count < max_retries:
                try:
                    response = session.post(fetch_data_golang, data=json.dumps(payload), headers=self.header)
                    print(payload)
                    status = response.status_code   
                    self.logger.info(f"Get ticket response: {status}", extra={'stage': '15'})

                    if status == 200:
                        print(f"url {fetch_data_golang}: Fetched successfully!")
                        return response.json().get("Tickets", [])
                    else:
                        retry_count += 1
                        print(f"url {fetch_data_golang}: Status {status}, Retrying {retry_count}/{max_retries}...")
                except Exception as e:
                    retry_count += 1
                    print(f"url {fetch_data_golang}: Error - {e}. Retrying {retry_count}/{max_retries}...")
            return []
    
        # Fetch data concurrently
        org_tickets = []
        session = requests.Session()
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_page = {executor.submit(fetch_data, page, session): page for page in range(1, pages_in_fetch_api + 1)}
            for future in as_completed(future_to_page):
                page = future_to_page[future]
                try:
                    data = future.result()
                    if data:
                        org_tickets.extend(data)
                except Exception as e:
                    print(f"Page {page}: Exception occurred - {e}")
        
        print(f"Total tickets data fetched: {len(org_tickets)}")
        return org_tickets

    def create_ticket_golang(self, org_id, payload, log_payload):
        create_url = self.golang_url.rstrip("/") + f"/tickets/bulkCreate-tickets?forData=true&organization_id={org_id}"
        print(create_url)

        max_retries = 5
        retry_delay = 10
        attempt = 0

        while attempt < max_retries:
            try:
                log_payload['count'] = None
                log_payload['response_status'] = None
                log_payload['response_message'] = None
                log_payload['exception'] = None
                
                response = requests.post(create_url, headers=self.header, data=json.dumps(payload))
                status = response.status_code
                
                log_payload['response_status'] = status
                log_payload['response_message'] = response.json()
                log_payload['count'] = response.json()['created_tickets']
                
                self.logger.info(f"Ticket creation response: {status}", extra={'stage': '15'})
                self.logger.info(f"Created tickets {response.json()['created_tickets']}", extra={'stage': '15'})

                failed_tickets = response.json().get('failed_tickets', 0)
                failed_messages = response.json().get('failed_tickets_due', [])

                if status == 429:
                    self.logger.warning(f"Rate limited. Retrying after {retry_delay} seconds...", extra={'stage': '15'})
                    time.sleep(retry_delay)
                    attempt += 1
                    continue

                if status >= 400:
                    self.logger.error(f"Request failed with status {status}", extra={'stage': '15'})
                    self.logger.error(f"Request resposne {response.text}", extra={'stage': '15'})
                    raise Exception(f"Request failed with status {status}: {response.text}", extra={'stage': '15'})
                
                if failed_tickets > 0:
                    error_message = failed_messages[1]['message'] if len(failed_messages) > 1 and 'message' in failed_messages[1] else "No detailed message"
                    ticket_word = "ticket was" if failed_tickets == 1 else "tickets were"

                    self.logger.warning(
                        f"{failed_tickets} {ticket_word} not created with reason: {error_message}",
                        extra={'stage': '15'}
                    )
                    
                    log_payload['exception'] = f"{failed_tickets} {ticket_word} not created with reason: {error_message}"
                    
                
                self.log_writer(log_payload)
                
                return response

            except Exception as err:
                self.logger.error(f"Failed to create tickets on attempt {attempt + 1}: {err}", extra={'stage': '15'})
                log_payload['exception'] = f"Failed to create tickets on attempt {attempt + 1}: {err}"
                
                self.log_writer(log_payload)
                
                attempt += 1
                time.sleep(retry_delay)

        self.logger.error(f"Ticket creation failed after multiple retries due to repeated errors.", extra={'stage': '15'})
        raise RuntimeError("Ticket creation failed after multiple retries due to repeated errors.")
    
    def status_change_golang(self, status, dataframe, log_payload):
        import json
        import time
        import requests

        full_data_list = []
        for index, row in dataframe.iterrows():
            status_comments = {
                                'closed': 'Astrom RPA Closed',
                                'reopened': 'Astrom RPA Reopened',
                                'solved': 'Astrom RPA Solved'
                                }

            comment = status_comments.get(status)
                
            payload = {
                "ticketId" : row["_id"],
                "status" : status,
                "comment" : comment
            }
            full_data_list.append(payload)
        print(f"Number Of Tickets To Be {status.upper()}:", len(full_data_list))

        status_change_golang_api = self.golang_url.rstrip("/") + f"/tickets/status-update-tickets-bulk"
        ticket_counter = 0
        chunk_size = 100

        for i in range(0, len(full_data_list), chunk_size):
            chunk = full_data_list[i:i+chunk_size]
            res_status = 0
            print(f"Chunk Of {status.upper()} Tickets:", len(chunk))
            print(f"Payload Chunk To Be {status.upper()}: ", chunk)
            while res_status != 200:
                try:
                    log_payload['count'] = None
                    log_payload['response_status'] = None
                    log_payload['response_message'] = None
                    log_payload['exception'] = None
                    
                    status_change_api_golang = requests.post(status_change_golang_api, data=json.dumps(chunk), headers=self.header)
                    res_status = status_change_api_golang.status_code
                    
                    log_payload['response_status'] = res_status
                    log_payload['response_message'] = status_change_api_golang.text

                    log_payload['count'] = len(chunk)
                    
                    print(f"{status.upper()} Ticket API Status Code: ", res_status)
                    if res_status == 200:
                        ticket_counter = ticket_counter + len(chunk)
                        print(f"Total Tickets {status.upper()} Till Now", ticket_counter)
                    elif res_status == 429:
                        retry_delay = 10
                        print(status_change_api_golang.text)
                        self.logger.warning(f"Rate limited. Retrying after {retry_delay} seconds...", extra={'stage': '15'})
                        time.sleep(retry_delay)
                    else:
                        print(status_change_api_golang.text)

                        
                    self.log_writer(log_payload)

                except Exception as e:
                    log_payload['exception'] = e
                    print(f"Error: {e}")
                    self.log_writer(log_payload)

        print(f"All Tickets Have Been {status.upper()}")