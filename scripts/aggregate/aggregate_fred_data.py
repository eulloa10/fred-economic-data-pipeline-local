import logging
import os
from typing import Dict, Optional, List
import io
import pandas as pd
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from airflow.hooks.S3_hook import S3Hook
import pandas.errors

load_dotenv(os.path.join(os.path.dirname(__file__), '../../.env'))

S3_DATA_LAKE = os.getenv('S3_DATA_LAKE')

logger = logging.getLogger(__name__)

class FredDataAggregator:
    def __init__(self,
                 s3_bucket: str = S3_DATA_LAKE,
                 s3_conn_id: str = 'aws_default'):
        """
        Initialize the FRED data aggregator

        :param s3_bucket: S3 bucket to store the data
        """
        logger.info("Initializing FREDDataAggregator with S3 bucket: %s", s3_bucket)
        if not s3_bucket:
            raise ValueError("S3 bucket name is not set.")
        self.s3_bucket = s3_bucket
        self.s3_hook = S3Hook(aws_conn_id=s3_conn_id)

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
        except pandas.errors.ParserError as e:
            logger.error("Error parsing Parquet file from S3: %s", e)
            return pd.DataFrame()
        except Exception as e:
            logger.error("Error reading Parquet file from S3: %s", e)
            return pd.DataFrame()

        except Exception as e:
            logger.error("Error reading Parquet file from S3: %s", e)
            return pd.DataFrame()

    def save_parquet_to_s3(self, df: pd.DataFrame, s3_path: str) -> None:
        """
        Save a DataFrame as a Parquet file to S3

        :param df: DataFrame to save
        :param s3_path: S3 path to save the Parquet file
        """
        logger.info("Saving Parquet file to S3: %s", s3_path)
        try:
            if df.empty:
                logger.warning("DataFrame is empty. Not saving to S3.")
                return

            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            self.s3_hook.load_bytes(buffer.getvalue(), key=s3_path, bucket_name=self.s3_bucket, replace=True)
            logger.info("Saved Parquet file to S3 successfully")
        except ClientError as e:
            logger.error(f"Error saving Parquet file to S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Error saving Parquet file to S3: {e}")
            raise

    def aggregate_data(self,
                       series_id: str,
                       start_year: int,
                       end_year: int) -> Optional[List[str]]:
        """
        Aggregate the data for a given series ID and year range
        :param series_id: FRED series ID
        :param start_year: Overall start year
        :param end_year: Overall end year
        :return: List of S3 paths where the aggregated data is stored
        """
        logger.info("Aggregating data for series ID %s from %s to %s", series_id, start_year, end_year)
        years = [year for year in range(start_year, end_year + 1)]
        months = [x for x in range(1, 13)]
        s3_paths = []

        try:
            for year in years:
              yearly_data = pd.DataFrame()
              for month in months:
                  processed_data_path = f'processed_data/indicator={series_id}/year={year}/month={month}/{series_id}_{year}_{month}.parquet'
                  logger.info("Reading processed data from S3: %s", processed_data_path)
                  monthly_data = self.read_parquet_from_s3(processed_data_path)

                  if not monthly_data.empty:
                        yearly_data = pd.concat([yearly_data, monthly_data], ignore_index=True)
                  else:
                      logger.warning("No data found for %s in year %s, month %02d.", series_id, year, month)

              if yearly_data.empty:
                  logger.warning("No data found for %s for year %s", series_id, year)
                  continue

              yearly_data['aggregated_at'] = pd.Timestamp.now(tz='UTC').isoformat()
              yearly_data['value'] = yearly_data['value'].round(2)
              s3_path = f'aggregated_data/indicator={series_id}/year={year}/{series_id}_{year}.parquet'
              self.save_parquet_to_s3(yearly_data, s3_path)
              s3_paths.append(s3_path)
              logger.info("Saved aggregated data to S3: %s", s3_path)

            logger.info("Completed aggregation for series ID %s from %s to %s", series_id, start_year, end_year)
            return s3_paths

        except Exception as e:
            logger.error("Error aggregating data for series ID %s from %s to %s: %s", series_id, start_year, end_year, e)
            return None

def aggregate_fred_indicator_processed_data(
    series_id: str,
    start_year: int,
    end_year: int
) -> Optional[List[str]]:
    """
    Aggregate the data for a given series ID and year range
    :param series_id: FRED series ID
    :param start_year: Overall start year
    :param end_year: Overall end year
    :return: List of S3 paths where the aggregated data is stored
    """
    logger.info("Aggregating data for series ID %s from %s to %s", series_id, start_year, end_year)
    aggregator = FredDataAggregator()
    return aggregator.aggregate_data(series_id, start_year, end_year)
