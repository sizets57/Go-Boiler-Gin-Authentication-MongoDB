import logging
import requests
import json
import re
import configparser
import os
from MasterDag import MasterDag

dag_instance = MasterDag()
config = dag_instance.GetConfig()

class DigitalIpom:
    def __init__(self):
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - Stage %(stage)s - %(name)s - %(levelname)s - %(message)s")
        
        self.logger = logging.getLogger("ipom_utils")

        # path = os.path.join(os.getenv("AIRFLOW_HOME", "/opt/airflow"), f"dags/IPOM_Dags/config.ini")
        # path = "dags/Astrom_Lite/config.ini"
        # config = configparser.ConfigParser()

        self.logger.info("Reading config file for fetching Digital IPOM related data...", extra={'stage': '1'})
        # config.read(path)

        self.base_url = config['astrum']['base_url']
        self.astrum_user = config['astrum']['user']
        self.astrum_password = config['astrum']['password']

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
    
    def get_ticketing_urls(self, token):
        headers = {
        'Authorization': f'Bearer {token}'
        }

        url = self.base_url.rstrip("/") + "/config/base-urls"
        response = requests.get(url, headers=headers)
        self.logger.info(f"Status: {response.status_code}", extra={'stage': '10'})
        response.raise_for_status()

        return response

    def get_ipom_details(self, token, ipom_id):
        headers = {
        'Authorization': f'Bearer {token}'
        }

        url = self.base_url.rstrip("/") + f"/ipom/show/{ipom_id}"
        response = requests.get(url, headers=headers)
        self.logger.info(f"Status: {response.status_code}", extra={'stage': '3'})
        response.raise_for_status()
        
        return response

    def get_connection_details(self, token, connection_id):
        headers = {
        'Authorization': f'Bearer {token}'
        }
    
        url = self.base_url.rstrip("/") + f"/connection/show/{connection_id}"
        response = requests.get(url, headers=headers)
        self.logger.info(f"Status: {response.status_code}", extra={'stage': '4'})
        response.raise_for_status()
        
        return response

    def OracleConnection(self, username, password, hostname, port, service=None):
        import sys
        import oracledb
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool
        
        try:
            dialect = 'oracle'
            oracledb.version = "8.3.0"
            sys.modules["cx_Oracle"] = oracledb
            service_part = f"/?service_name={service}" if service else "/FREEPDB1"
            engine = dialect+'://'+username+':'+password+'@'+hostname+':'+port+service_part
            print(engine)
            return engine

        except Exception as e:
            self.logger.error(f"Failed for Oracle DB: {str(e)}", exc_info=True, extra={'stage': '5'})
            raise

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
            return connection_uri

        except Exception as e:
            self.logger.error(f"Failed for SQL Server: {str(e)}", exc_info=True, extra={'stage': '5'})
            raise
            
    def get_db_engine(self, db_type, user, password, host, port, db_name):
        self.logger.info(f"Fetching Connection URI", extra={'stage': '5'})
        if db_type.lower() == "oracle":
            return self.OracleConnection(user, password, host, port, db_name)
        elif db_type.lower() == "mssql":
            return self.SQLConnection(f"{host},{port}", db_name, user, password)
        else:
            self.logger.error(f"Unsupported DB type: {db_type}", extra={'stage': '5'})
            raise ValueError(f"Unsupported DB type: {db_type}")
        
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
    
    def decrypt(self, enc_text, key = "AhzUuSqlk0v0WVXDmtPu0xSTFjsdRSpI"):
        import base64
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        
        self.logger.info("Decrypting connection password", extra={'stage': '4'})
        
        try:
            data = base64.b64decode(enc_text)

            key_bytes = key.encode("utf-8")

            aesgcm = AESGCM(key_bytes)

            nonce_size = 12
            if len(data) < nonce_size:
                raise ValueError("Encrypted data is too short to contain nonce")

            nonce, ciphertext = data[:nonce_size], data[nonce_size:]

            plaintext = aesgcm.decrypt(nonce, ciphertext, None)

            return plaintext.decode("utf-8")

        except Exception as e:
            self.logger.error("Error occured decrypting password: {e}", extra={'stage': '4'})
            raise RuntimeError(f"Failed to decrypt: {e}")
