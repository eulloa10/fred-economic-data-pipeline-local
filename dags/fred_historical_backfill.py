from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yaml
import os
import logging

from scripts.extract.extract_fred_data import extract_fred_indicator
from scripts.transform.transform_fred_data import transform_fred_indicator_raw_data
from scripts.aggregate.aggregate_fred_data import aggregate_fred_indicator_processed_data
from scripts.serving.load_fred_data import load_to_rds
from scripts.serving.load_fred_data_to_google import load_to_google_sheet

def load_indicators_config():
    """
    Load indicators from a YAML configuration file
    """
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        'config',
        'fred_indicators.yaml'
    )

    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def generate_historical_backfill_dags():
    """
    Generate historical backfill DAGs for all indicators
    """
    indicators_config = load_indicators_config()
    dags = []
    for indicator in indicators_config['indicators']:
        dags.append(create_fred_historical_backfill_dag(indicator))
    return dags

def create_fred_historical_backfill_dag(indicator_config):
    """
    Create a DAG for historical backfill of FRED data
    """
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2010, 1, 1),
        'end_date': datetime(2010, 12, 31),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

    dag_id = f'fred_historical_backfill_{indicator_config["series_id"]}'

    with DAG(
        dag_id,
        default_args=default_args,
        description=f'Historical backfill for {indicator_config.get("name", indicator_config["series_id"])}',
        schedule_interval='@monthly',
        catchup=True
    ) as dag:

        def extract_monthly_task(series_id, start_date, end_date, **kwargs):
            """
            Extract data for a specific month
            """
            try:
                extract_fred_indicator(series_id=series_id, start_date=start_date, end_date=end_date)
            except Exception as e:
                logging.getLogger(__name__).error("Monthly extraction failed for %s in %s: %s", series_id, start_date, e)
                raise

        def transform_monthly_task(series_id, start_date, end_date, **kwargs):
            """
            Transform data for a specific month
            """
            try:
                transform_fred_indicator_raw_data(series_id=series_id, start_date=start_date, end_date=end_date)
            except Exception as e:
                logging.getLogger(__name__).error("Monthly transformation failed for %s: %s", series_id, e)
                raise

        def aggregate_yearly_task(series_id, year, **kwargs):
            """
            Aggregate monthly data for a specific year
            """
            try:
                year = int(year)
                aggregate_fred_indicator_processed_data(series_id=series_id, start_year=year, end_year=year)
            except Exception as e:
                logging.getLogger(__name__).error("Yearly aggregation failed for %s in %s: %s", series_id, year, e)
                raise

        def load_yearly_task(series_id, year, table_name, **kwargs):
            """
            Load aggregated yearly data
            """
            try:
                year = int(year)
                load_to_rds(series_id=series_id, start_year=year, end_year=year, table_name=table_name)
            except Exception as e:
                logging.getLogger(__name__).error("Yearly loading failed for %s in %s: %s", series_id, year, e)
                raise

        def load_to_google_sheet_yearly_task(series_id, year, sheet_name, **kwargs):
            """
            Load aggregated yearly data
            """
            try:
                year = int(year)
                load_to_google_sheet(series_id=series_id, start_year=year, end_year=year, google_sheet_name=sheet_name)
            except Exception as e:
                logging.getLogger(__name__).error("Yearly loading failed for %s in %s: %s", series_id, year, e)
                raise

        execution_date = "{{ execution_date }}"
        start_date = "{{ execution_date.replace(day=1).strftime('%Y-%m-%d') }}"
        end_date = "{{ (execution_date + macros.timedelta(days=32)).replace(day=1) - macros.timedelta(days=1) }}"
        year = "{{ execution_date.year | int }}"

        extract_monthly = PythonOperator(
            task_id='extract_monthly',
            python_callable=extract_monthly_task,
            op_kwargs={
                'series_id': indicator_config['series_id'],
                'start_date': start_date,
                'end_date': end_date,
            },
        )

        transform_monthly = PythonOperator(
            task_id='transform_monthly',
            python_callable=transform_monthly_task,
            op_kwargs={
                'series_id': indicator_config['series_id'],
                'start_date': start_date,
                'end_date': end_date,
            },
        )

        aggregate_yearly = PythonOperator(
            task_id='aggregate_yearly',
            python_callable=aggregate_yearly_task,
            op_kwargs={
                'series_id': indicator_config['series_id'],
                'year': year,
            },
            trigger_rule='all_success'
        )

        load_yearly = PythonOperator(
            task_id='load_yearly',
            python_callable=load_yearly_task,
            op_kwargs={
                'series_id': indicator_config['series_id'],
                'year': year,
                'table_name': indicator_config['table_name'],
            },
            trigger_rule='all_success'
        )

        load_google_sheet_yearly = PythonOperator(
            task_id='load_google_sheet_yearly',
            python_callable=load_to_google_sheet_yearly_task,
            op_kwargs={
                'series_id': indicator_config['series_id'],
                'year': year,
                'sheet_name': indicator_config['sheet_name'],
            },
            trigger_rule='all_success'
        )

        extract_monthly >> transform_monthly >> aggregate_yearly >> load_yearly >> load_google_sheet_yearly

    return dag

dags = generate_historical_backfill_dags()
for dag in dags:
    globals()[dag.dag_id] = dag
