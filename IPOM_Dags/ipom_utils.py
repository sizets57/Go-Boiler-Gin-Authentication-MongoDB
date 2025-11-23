import logging
import requests
import json
import re
from MasterDag import MasterDag

dag_instance = MasterDag()
config = dag_instance.GetConfig()

class DigitalIpom:
    def __init__(self):
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - Stage %(stage)s - %(name)s - %(levelname)s - %(message)s")

        self.logger = logging.getLogger("ipom_utils")
        self.logger.info("Reading config file for fetching Digital IPOM related data...", extra={'stage': '1'})

        self.base_url = config['astrum']['base_url']
        self.astrum_user = config['astrum']['user']
        self.astrum_password = config['astrum']['password']
        self.astrum_instance = config['astrum']['instance']

    # ---------------------------
    # Astrum API Calls
    # ---------------------------
    
    def astrum_login(self):
        payload = json.dumps({
            "email": self.astrum_user,
            "password": self.astrum_password
        })

        headers = {'Content-Type': 'application/json'}

        url = self.base_url.rstrip("/") + "/login"
        response = requests.post(url, headers=headers, data=payload)
        self.logger.info(f"Status: {response.status_code}", extra={'stage': '2'})
        response.raise_for_status()
        
        return response
    
    def fetch_token(self):
        res = None
        try:
            self.logger.info("Fetching token to login Astrum-lite", extra={'stage': '2'})
            res = self.astrum_login()
            res.raise_for_status()
            content = res.json()
            data = content.get("data")
            token = data.get("token") if data and 'token' in data else None
            if not token:
                raise ValueError("Token not found in login response.")
            return token
        except Exception as e:
            self.logger.error(f"Login failed: {e}", extra={'stage': '2'})
            if res is not None:
                self.logger.error(f"Response: {res.text}", extra={'stage': '2'})
            raise
    
    def get_ticketing_urls(self):
        token = self.fetch_token()
        
        headers = {
        'Authorization': f'Bearer {token}'
        }

        url = self.base_url.rstrip("/") + "/config/base-urls"
        print(url)
        response = requests.get(url, headers=headers)
        self.logger.info(f"Status: {response.status_code}", extra={'stage': '10'})
        response.raise_for_status()

        return response
    
    def fetch_ticketing_urls(self, token):
        res = None
        try:
            self.logger.info("Fetching ticketing base urls", extra={'stage': '10'})
            res = self.get_ticketing_urls()
            res.raise_for_status()
            content = res.json()
            conn = content.get("data")
            return conn
        
        except Exception as e:
            self.logger.error(f"Unexpected error while fetching URLs:: {e}", extra={'stage': '10'})
            if res is not None:
                self.logger.error(f"Response: {res.text}", extra={'stage': '10'})
            raise

    def get_ipom_details(self, token, ipom_id):
        headers = {
        'Authorization': f'Bearer {token}'
        }

        # url = self.base_url.rstrip("/") + f"/ipom/show/{ipom_id}"
        config = self.astrum_instance
        url = self.base_url.rstrip("/") + f"/ipom/show/{ipom_id}?config={config}"
        response = requests.get(url, headers=headers)
        self.logger.info(f"Status: {response.status_code}", extra={'stage': '3'})
        response.raise_for_status()
        
        return response
    
    def fetch_ipom_details(self, token, ipom_id):
        res = None
        try:
            self.logger.info("Fetching IPOM details", extra={'stage': '3'})
            res = self.get_ipom_details(token, ipom_id)
            res.raise_for_status()
            content = res.json()
            data = content.get("data")
            details = data.get("ipom")
            if not details:
                raise ValueError("IPOM details not found.")
            return details
        except Exception as e:
            self.logger.error(f"Failed to fetch IPOM details: {e}", extra={'stage': '3'})
            if res is not None:
                self.logger.error(f"Response: {res.text}", extra={'stage': '2'})
            raise

    def get_connection_details(self, token, connection_id):
        headers = {
        'Authorization': f'Bearer {token}'
        }
    
        url = self.base_url.rstrip("/") + f"/connection/show/{connection_id}"
        response = requests.get(url, headers=headers)
        self.logger.info(f"Status: {response.status_code}", extra={'stage': '4'})
        response.raise_for_status()
        
        return response
    
    def fetch_connection(self, token, connection_id):
        res = None
        try:
            self.logger.info("Fetching DB connection details", extra={'stage': '4'})
            res = self.get_connection_details(token, connection_id)
            res.raise_for_status()
            content = res.json()
            conn = content.get("data")
            return conn
        except Exception as e:
            self.logger.error(f"Failed to fetch DB connection: {e}", extra={'stage': '4'})
            if res is not None:
                self.logger.error(f"Response: {res.text}", extra={'stage': '2'})
            raise
        
    # ---------------------------
    # Database Connection Builders
    # ---------------------------

    def calling_get_db_engine(self, db_conn):
        import os
        
        password = db_conn.get('password', None)
        if password:
            password = str(dag_instance.decrypt(password))
        
        DAGS_FOLDER = os.getenv("AIRFLOW_DAGS_FOLDER", "/opt/airflow/dags")
        
        key_file = db_conn.get('secretKey', None)
        
        if key_file:
            key_file = os.path.basename(key_file)
    
        full_key_path = os.path.join(DAGS_FOLDER, key_file) if key_file else None

        db_engine = dag_instance.get_db_engine(db_type=db_conn.get('type'),
                                    user=db_conn.get('userName'),
                                    password=password,
                                    host=str(db_conn.get('hostName')),
                                    port=str(db_conn.get('port', None)),
                                    db_name=db_conn.get('databaseName'),
                                    warehouse=db_conn.get('snowflakeWarehouse', None),
                                    key_path=full_key_path,
                                    role=db_conn.get('snowflakeRole', None),
                                    schema=db_conn.get('schema', None))
        return db_engine

    def check_column_uniqness(self, df, ipom_dynamic_fields):
        self.logger.info(f"Checking uniqness of UID column", extra={'stage': '6'})
        unique_col_name = next((item["column"] for item in ipom_dynamic_fields if item.get("uuid") is True),None).lower()
        print("Unique Column From DB", unique_col_name)
        
        uniquness = df[unique_col_name].is_unique
        
        if uniquness:
            self.logger.info(f"Column '{unique_col_name}' passed uniqueness check.", extra={'stage': '6'})
        else:
            duplicate_count = df[unique_col_name].duplicated().sum()
            error_msg = f"❌ Column '{unique_col_name}' is not unique. Found {duplicate_count} duplicate rows."
            self.logger.error(error_msg, extra={'stage': '6'})
            raise ValueError(error_msg)
        
    def load_and_prepare_dataframe(self, engine, ipom_details, airflow_ti = None):
        import pandas as pd
        try:
            self.logger.info(f"Extracting data from source table", extra={'stage': '6'})
            query = ipom_details.get('query')
            df = pd.read_sql(query, engine)
            df.columns = df.columns.str.lower()
            self.check_column_uniqness(df, ipom_details["dynamicFieldsUpdateTicket"])
        except Exception as err:
            self.logger.error(f"Data extraction failed: {err}", extra={'stage': '6'})
            raise
        else:
            return df
    
    # ---------------------------
    # Condition Helpers
    # ---------------------------
    
    @staticmethod
    def to_snake_case(s):
        return re.sub(r'\s+', '_', s.strip().lower())

    @staticmethod
    def quote_if_needed(value):
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        try:
            float(value)
            return value
        except ValueError:
            value = value.replace('"', '\\"')
            return f'"{value}"'

    def sort_rules(self, rules):
        self.logger.info(f"Sorting the rules for ticketing...", extra={'stage': '15'})
        order = ["close", "open", "reopen", "solve"]
        
        sorted_rules = sorted(rules, key=lambda rule: order.index(rule.get("ruleName", "")))
        
        return sorted_rules

    def fix_condition(self, condition):
        parts = re.split(r'\s+(and|or)\s+', condition.strip(), flags=re.IGNORECASE)

        fixed_parts = []
        for i in range(0, len(parts), 2):
            part = parts[i]
            if not part.strip():
                continue

            match = re.match(r'([a-zA-Z\s]+?)\s*(=|>=|<=|!=|>|<)\s*(.+)', part.strip())
            if match:
                col, op, val = match.groups()
                col_fixed = self.to_snake_case(col)
                op_fixed = '==' if op == '=' else op
                val_fixed = self.quote_if_needed(val)
                fixed_parts.append(f"{col_fixed} {op_fixed} {val_fixed}")
            else:
                fixed_parts.append(part.strip())

        connectors = parts[1::2]
        result = fixed_parts[0]
        for connector, clause in zip(connectors, fixed_parts[1:]):
            result += f" {connector.lower()} {clause}"

        return result
    
    def build_condition(self, df, cond):
        import operator
        
        op_map = {
            'less_than': operator.lt,
            'greater_than': operator.gt,
            'equal_to': operator.eq,
            'not_equal_to': operator.ne,
            'less_than_equal': operator.le,
            'greater_than_equal': operator.ge
        }
        
        column = cond['column'].lower()
        op_key = cond['operator']
        value = cond['value']

        if op_key not in op_map:
            self.logger.error(f"Unsupported operator: {op_key}", extra={'stage': '7'})
            raise ValueError(f"Unsupported operator: {op_key}")
        
        if column not in df.columns:
            self.logger.error(f"Column '{column}' not found in DataFrame", extra={'stage': '7'})
            raise ValueError(f"Column '{column}' not found in DataFrame")
        
        try:
            value = float(value)
        except ValueError:
            pass

        op_func = op_map[op_key]
        return op_func(df[column], value)

    def combine_conditions(self, conditions, logic):
        if not conditions:
            self.logger.error("No conditions provided to combine", extra={'stage': '7'})
            raise ValueError("No conditions provided to combine")

        logic = logic.lower()
        final = conditions[0]

        for cond in conditions[1:]:
            if logic == 'any':
                final |= cond
            elif logic == 'all':
                final &= cond
            else:
                self.logger.error(f"Invalid condition logic: {logic}", extra={'stage': '7'})
                raise ValueError(f"Invalid condition logic: {logic}")

        return final
