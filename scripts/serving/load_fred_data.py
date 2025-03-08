import logging
import os
from typing import Optional, List
import io
import boto3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from botocore.exceptions import ClientError

load_dotenv('../../.env')

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
class FREDDataLoader:
    def __init__(self,
                 db_params: dict,
                 s3_bucket: str = os.getenv('S3_DATA_LAKE')):
        """
        Initialize the FRED data loader
        :param db_params: Database connection parameters
        """
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3')
        self.db_params = db_params
        self.engine = self._create_engine()

        if not self.s3_bucket:
            raise ValueError("S3 bucket name is not set. Please set the S3_DATA_LAKE environment variable.")
        if not self.db_params:
            raise ValueError("Database connection parameters are not set. Please provide the database connection parameters.")
        logger.info("FREDDataLoader initialized with S3 bucket: %s", self.s3_bucket)

    def _create_engine(self):
        """
        Create a SQLAlchemy engine for database connection
        """
        try:
            db_url = f"postgresql+psycopg2://{self.db_params['user']}:{self.db_params['password']}@{self.db_params['host']}:{self.db_params['port']}/{self.db_params['database']}"
            engine = create_engine(db_url)
            logger.info("SQLAlchemy engine created successfully.")
            return engine
        except Exception as e:
            logger.error("Error creating SQLAlchemy engine: %s", e)
            raise

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
                return

            records = dataframe.to_dict('records')
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=self.engine)

            with self.engine.begin() as connection:
                insert_stmt = insert(table).values(records)

                upsert_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=unique_columns,
                    set_={
                        col: insert_stmt.excluded[col]
                        for col in records[0].keys()
                        if col not in unique_columns
                    }
                )
                connection.execute(upsert_stmt)

            logger.info("Data upserted successfully into table: %s", table_name)
            return len(records)

        except Exception as e:
            logger.error("Error upserting data into table %s: %s", table_name, e)
            raise

    def read_data_from_db(self, query: str) -> pd.DataFrame:
        """
        Read data from the database using a SQL query.

        :param query: SQL query to execute.
        :return: DataFrame containing the query results.
        """
        try:
            df = pd.read_sql_query(query, self.engine)
            logger.info("Data read successfully from the database.")
            return df
        except Exception as e:
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
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_path)
            df = pd.read_parquet(io.BytesIO(response['Body'].read()))
            logger.info("Read Parquet file from S3 successfully")
            return df

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning("File not found at %s. Returning empty DataFrame.", s3_path)
            else:
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
                    dataframe=df,
                    table_name=table_name,
                    unique_columns=unique_columns
                )

                logger.info("Loaded %s records from S3 path %s to table: %s", records_loaded, aggregated_s3_data_path, table_name)
                successful_s3_paths_loaded.append(aggregated_s3_data_path)

            return successful_s3_paths_loaded

        except Exception as e:
            logger.error("Error loading data from S3 to RDS: %s", e)
            raise

def load_to_rds(series_id: str, start_year: int, end_year: int, table_name: str) -> Optional[List[str]]:
    """
    Load data to RDS

    :param series_id: FRED series identifier
    :param start_year: Starting year for data
    :param end_year: Ending year for data
    :param table_name: Name of the table in the database
    """
    db_params = {
        'host': os.getenv('RDS_HOST'),
        'database': os.getenv('RDS_DATABASE'),
        'user': os.getenv('RDS_USER'),
        'password': os.getenv('RDS_PASSWORD'),
        'port': os.getenv('RDS_PORT')
    }

    loader = FREDDataLoader(db_params)

    return loader.load_from_s3_to_rds(
            series_id=series_id,
            start_year=start_year,
            end_year=end_year,
            table_name=table_name
            )

if __name__ == "__main__":
    result = load_to_rds(
        series_id='UNRATE',
        start_year=2017,
        end_year=2018,
        table_name='economic_indicators'
    )
    if result:
        logger.info("Successfully loaded data to RDS from the following S3 paths: %s", result)
    else:
        logger.error("Failed to load data to RDS.")
