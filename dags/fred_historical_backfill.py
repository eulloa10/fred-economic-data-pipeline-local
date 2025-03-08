from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import yaml
import os
import logging

from scripts.extract.extract_fred_data import extract_fred_indicator
from scripts.transform.transform_fred_data import transform_fred_indicator_raw_data
from scripts.aggregate.aggregate_fred_data import aggregate_fred_indicator_processed_data
from scripts.serving.load_fred_data import load_to_rds

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

def create_fred_historical_backfill_dag(indicator_config):
    """
    Create a DAG for historical backfill of FRED data
    """
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2000, 1, 1),
        'end_date': datetime(2025, 2, 28),
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

        def extract_monthly_task(**context):
            """
            Extract data for a specific month
            """
            execution_date = context['execution_date']

            start_date = execution_date.replace(day=1).strftime('%Y-%m-%d')
            end_date = (execution_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            end_date = end_date.strftime('%Y-%m-%d')

            series_id = indicator_config['series_id']

            try:
                extract_fred_indicator(
                    series_id=series_id,
                    start_date=start_date,
                    end_date=end_date
                )

            except Exception as e:
                logging.error("Monthly extraction failed for %s in %s: %s", series_id, start_date, e)
                raise

        def transform_monthly_task(**context):
            """
            Transform data for a specific month
            """
            execution_date = context['execution_date']

            start_date = execution_date.replace(day=1).strftime('%Y-%m-%d')
            end_date = (execution_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            end_date = end_date.strftime('%Y-%m-%d')

            series_id = indicator_config['series_id']

            try:
                transformed_data = transform_fred_indicator_raw_data(
                    series_id=series_id,
                    start_date=start_date,
                    end_date=end_date
                )

            except Exception as e:
                logging.error("Monthly transformation failed for %s: %s", series_id, e)
                raise

        def aggregate_yearly_task(**context):
            """
            Aggregate monthly data for a specific year
            """
            execution_date = context['execution_date']
            year = execution_date.year

            series_id = indicator_config['series_id']

            try:
                aggregate_fred_indicator_processed_data(
                    series_id=series_id,
                    start_year=year,
                    end_year=year
                )

            except Exception as e:
                logging.error("Yearly aggregation failed for %s in %s: %s", series_id, year, e)
                raise

        def load_yearly_task(**context):
            """
            Load aggregated yearly data
            """
            execution_date = context['execution_date']
            year = execution_date.year

            series_id = indicator_config['series_id']
            table_name = indicator_config['table_name']

            try:
                load_to_rds(
                    series_id=series_id,
                    start_year=year,
                    end_year=year,
                    table_name=table_name
                )

            except Exception as e:
                logging.error("Yearly loading failed for %s in %s: %s", series_id, year, e)
                raise

        extract_monthly = PythonOperator(
            task_id='extract_monthly',
            python_callable=extract_monthly_task,
            provide_context=True,
            dag=dag
        )

        transform_monthly = PythonOperator(
            task_id='transform_monthly',
            python_callable=transform_monthly_task,
            provide_context=True,
            dag=dag
        )

        aggregate_yearly = PythonOperator(
            task_id='aggregate_yearly',
            python_callable=aggregate_yearly_task,
            provide_context=True,
            dag=dag,
            # Ensure this runs only after the last month of the year
            trigger_rule='all_done'
        )

        load_yearly = PythonOperator(
            task_id='load_yearly',
            python_callable=load_yearly_task,
            provide_context=True,
            dag=dag,
            # Ensure this runs only after successful aggregation
            trigger_rule='all_success'
        )

        # Define task dependencies
        extract_monthly >> transform_monthly >> aggregate_yearly >> load_yearly

    return dag

def generate_historical_backfill_dags():
    """
    Generate historical backfill DAGs for all indicators
    """
    indicators_config = load_indicators_config()

    for indicator in indicators_config['indicators']:
        globals()[f'historical_backfill_dag_{indicator["series_id"]}'] = create_fred_historical_backfill_dag(indicator)

generate_historical_backfill_dags()
