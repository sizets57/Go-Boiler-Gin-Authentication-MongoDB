import sys
import logging
import requests
import oracledb
import pandas as pd
from airflow import DAG
from datetime import datetime
from ETL_Dags.ETL_Dag_Class import MasterDag
from ETL_Dags.Data_Manipluator import loginToSystem, TransferData
from airflow.operators.python import PythonOperator

dag_instance = MasterDag()
config = dag_instance.GetConfig()

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb


def pipeline():
    pipelineId = "68a6c1275f59e529ceec81c1"

    baseUrl = config["Backend"]["baseUrl"]
    url = baseUrl + "/tableManangement/show/" + pipelineId

    headers = loginToSystem()
    response = requests.request("GET", url, headers=headers, data={})
    print(response.json())

    if response.status_code != 200:
        raise ValueError("Enable to fetch pipeline Failed", response.text)

    df = pd.json_normalize(response.json()["data"])
    TransferData(df, dag_instance)


# The DAG is automatically registered when the script is loaded
with DAG(
    "$pipelineName",
    start_date=datetime(2025, 5, 5),
    catchup=False,
    schedule="@pipelineFrequency",
) as dag:
    t1 = PythonOperator(task_id="FetchData", python_callable=pipeline)
    t1

ETL = pipeline()
