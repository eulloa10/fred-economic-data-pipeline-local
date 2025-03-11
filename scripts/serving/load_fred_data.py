import logging
import os
from typing import Optional, List
import io
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from botocore.exceptions import ClientError
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.exc import SQLAlchemyError

load_dotenv(os.path.join(os.path.dirname(__file__), '../../.env'))

S3_DATA_LAKE = os.getenv('S3_DATA_LAKE')

logger = logging.getLogger(__name__)

class FREDDataLoader:
    def __init__(self,
                 db_conn_id: str,
                 s3_bucket: str = S3_DATA_LAKE,
                 aws_conn_id: str = 'aws_default'):
        """
        Initialize the FRED data loader
        :param db_params: Database connection parameters
        """
        self.s3_bucket = s3_bucket
        self.s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        self.db_hook = PostgresHook(postgres_conn_id=db_conn_id)
        if not self.s3_bucket:
            raise ValueError("S3 bucket name is not set.")
        logger.info("FREDDataLoader initialized with S3 bucket: %s", self.s3_bucket)

    def load_data_to_db(self, dataframe: pd.DataFrame, table_name: str, unique_columns: List[str]) -> Optional[int]:
        """
        Load a pandas DataFrame into the database.

        :param dataframe: DataFrame to load.
        :param table_name: Name of the table in the database.
        :param unique_columns: Columns that define a unique record for upsert.
        """
        try:
            if dataframe is None or dataframe.empty:
                logger.warning("DataFrame is empty. No data to load into %s", table_name)
                return 0

            records = dataframe.to_dict('records')
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=self.db_hook.get_sqlalchemy_engine())

            with self.db_hook.get_sqlalchemy_engine().begin() as connection:
                insert_stmt = insert(table).values(records)
                upsert_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=unique_columns,
                    set_={col: insert_stmt.excluded[col] for col in records[0].keys() if col not in unique_columns}
                )
                connection.execute(upsert_stmt)

            logger.info("Data upserted successfully into table: %s", table_name)
            return len(records)

        except SQLAlchemyError as e:
            logger.error("Error upserting data into table %s: %s", table_name, e)
            raise

    def read_data_from_db(self, query: str) -> pd.DataFrame:
        """
        Read data from the database using a SQL query.

        :param query: SQL query to execute.
        :return: DataFrame containing the query results.
        """
        try:
            df = pd.read_sql_query(query, self.db_hook.get_sqlalchemy_engine())
            logger.info("Data read successfully from the database.")
            return df
        except SQLAlchemyError as e:
            logger.error("Error reading data from the database: %s", e)
            raise

    def read_parquet_from_s3(self, s3_path: str) -> pd.DataFrame:
        """
        Read a Parquet file from S3

        :param s3_path: S3 path to the Parquet file
        :return: DataFrame containing the data
        """
        logger.info("Reading Parquet file from S3: %s", s3_path)
        try:
            s3_object = self.s3_hook.get_key(key=s3_path, bucket_name=self.s3_bucket)
            if s3_object:
                df = pd.read_parquet(io.BytesIO(s3_object.get()['Body'].read()))
                logger.info("Read Parquet file from S3 successfully")
                return df
            else:
                logger.warning("File not found at %s. Returning empty DataFrame.", s3_path)
                return pd.DataFrame()
        except ClientError as e:
            logger.error("Error reading Parquet file from S3: %s", e)
            return pd.DataFrame()
        except Exception as e:
            logger.error("Error reading Parquet file from S3: %s", e)
            return pd.DataFrame()

    def load_from_s3_to_rds(self,
                            series_id: str,
                            start_year: int,
                            end_year: int,
                            table_name: str) -> Optional[List[str]]:
        """
        Load data from S3 to RDS.

        :param series_id: FRED series identifier
        :param start_year: Starting year for data extraction
        :param end_year: Ending year for data extraction
        :param table_name: Name of the table in the database
        :return: List of successfully loaded S3 paths
        """
        unique_columns = ['indicator', 'observation_year', 'observation_month']
        years = [year for year in range(start_year, end_year + 1)]
        successful_s3_paths_loaded = []

        try:
            for year in years:
                aggregated_s3_data_path = f'aggregated_data/indicator={series_id}/year={year}/{series_id}_{year}.parquet'

                logger.info("Loading data from S3 path: %s", aggregated_s3_data_path)
                df = self.read_parquet_from_s3(aggregated_s3_data_path)
                if df.empty:
                        logger.warning("No data found in %s. Skipping.", aggregated_s3_data_path)
                        continue

                records_loaded = self.load_data_to_db(
                    df,
                    table_name,
                    unique_columns
                )

                logger.info("Loaded %s records from S3 path %s to table: %s", records_loaded, aggregated_s3_data_path, table_name)
                successful_s3_paths_loaded.append(aggregated_s3_data_path)
            return successful_s3_paths_loaded

        except Exception as e:
            logger.error("Error loading data from S3 to RDS: %s", e)
            raise

def load_to_rds(series_id: str, start_year: int, end_year: int, table_name: str, db_conn_id: str = "postgres_default") -> Optional[List[str]]:
    """
    Load data to RDS

    :param series_id: FRED series identifier
    :param start_year: Starting year for data
    :param end_year: Ending year for data
    :param table_name: Name of the table in the database
    """
    loader = FREDDataLoader(db_conn_id=db_conn_id)

    return loader.load_from_s3_to_rds(
                series_id,
                start_year,
                end_year,
                table_name
            )
