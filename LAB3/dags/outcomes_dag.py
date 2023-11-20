from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import os

from etl_scripts.pipeline import transform_data, load_data, load_fact_data

SOURCE_URL = "https://data.austintexas.gov/api/views/9t4d-g238/rows.csv"
# --Partial data-- SOURCE_URL = "https://shelterdata.s3.amazonaws.com/shelter1000.csv"
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
CSV_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/downloads'
CSV_TARGET_FILE = CSV_TARGET_DIR+'/outcomes_{{ds}}.csv'

PQ_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/processed'

with DAG(
    dag_id="outcomes_dag",
    start_date=datetime(2023,11,1),
    schedule_interval = '@daily'
) as dag:
    
    extract = BashOperator(
        task_id="extract",
        bash_command= f"curl --create-dirs -o {CSV_TARGET_FILE} {SOURCE_URL}",
    )

    transform = PythonOperator(

        task_id="transform",
        python_callable=transform_data,
        op_kwargs = {
            'source_csv': CSV_TARGET_FILE,
            'target_dir': PQ_TARGET_DIR
        }
    )

    tableNames = ["animalinfo_dim", "date_dim", "outcomes_dim", "outcomesex_dim", "outcomesub_dim", "animalrecords_fct"]

    load_animals_dim = PythonOperator(
        task_id="load_animals_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': PQ_TARGET_DIR+'/animalInfo_dim.parquet',
            'table_name': 'animalinfo_dim',
            'key':'animal_id'
        }
    )

    load_date_dim = PythonOperator(
        task_id="load_date_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': PQ_TARGET_DIR+'/date_dim.parquet',
            'table_name': 'date_dim',
            'key':'date_id'
        }
    )

    load_outcomes_dim = PythonOperator(
        task_id="load_outcomes_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': PQ_TARGET_DIR+'/outcomes_dim.parquet',
            'table_name': 'outcomes_dim',
            'key':'outcome_id'
        }
    )

    load_outcomesex_dim = PythonOperator(
        task_id="load_outcomesex_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': PQ_TARGET_DIR+'/outcomesex_dim.parquet',
            'table_name': 'outcomesex_dim',
            'key':'outcomesex_id'
        }
    )

    load_outcomesub_dim = PythonOperator(
        task_id="load_outcomesub_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': PQ_TARGET_DIR+'/outcomesub_dim.parquet',
            'table_name': 'outcomesub_dim',
            'key':'outcomesub_id'
        }
    )

    load_animalrecords_fct = PythonOperator(
        task_id="load_animalrecords_fct",
        python_callable=load_fact_data,
        op_kwargs = {
            'table_file': PQ_TARGET_DIR+'/records_fct.parquet',
            'table_name': 'animalrecords_fct'
        }
    )

    

    extract >> transform >> [load_animals_dim, load_date_dim, load_outcomes_dim, load_outcomesex_dim, load_outcomesub_dim] >> load_animalrecords_fct