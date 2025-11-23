import os
import logging
import configparser
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, DateTime, Text
from datetime import datetime
from MasterDag import MasterDag

dag_instance = MasterDag()
config = dag_instance.GetConfig()

nutanixSql = config["NutanixSql"]

class SQLHandler(logging.Handler):
    def __init__(self, ipom_id, table_name='astrum_logs'):
        super().__init__()
        # path = os.path.join(os.getenv("AIRFLOW_HOME", "/opt/airflow"), f"dags/IPOM_Dags/config.ini")
        # path = "dags/Astrom_Lite/config.ini"
        # config = configparser.ConfigParser()
        # config.read(path)
        # hostname = config['MySQL']['host']
        # username = config['MySQL']['username']
        # password = config['MySQL']['password']
        # port = config['MySQL']['port']
        # dbName = config['MySQL']['dbName']
        
        # db_uri=f"mysql+pymysql://{username}:{password}@{hostname}:{port}/{dbName}"

        self.engine = dag_instance.SQLConnection(nutanixSql["serverName"],"astrom", nutanixSql["userName"], nutanixSql["password"])
       # self.engine = create_engine(db_uri)
        self.table_name = table_name
        self.metadata = MetaData()
        self.log_table = Table(
            self.table_name, self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('ipom_id', String(50)),
            Column('timestamp', DateTime, default=datetime.utcnow),
            Column('level', String(50)),
            Column('module', String(100)),
            Column('function', String(100)),
            Column('stage', String(20)),
            Column('message', Text)
        )
        self.metadata.create_all(self.engine)
        self.ipom_id = ipom_id

    def emit(self, record):
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    self.log_table.insert(),
                    {
                        'ipom_id': self.ipom_id,
                        'timestamp': datetime.utcnow(),
                        'level': record.levelname,
                        'module': record.name,
                        'function': record.funcName,
                        'stage': getattr(record, 'stage', None),
                        'message': self.format(record)
                    }
                )
        except Exception:
            self.handleError(record)
