import logging
import os
from typing import Dict, Optional, List
import io
import boto3
import pandas as pd
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv('../../.env')

S3_DATA_LAKE = os.getenv('S3_DATA_LAKE')

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class YearRangeGenerator:
    @staticmethod
    def generate_years(start_year: int,
                       end_year: int
                      ) -> List[int]:
        """
        Generate a list of years for a given range

        :param start_year: Overall start year
        :param end_year: Overall end year
        :return: List of years
        """
        logger.info("Generating year range from %s to %s", start_year, end_year)

        try:
            years = list(range(start_year, end_year + 1))
            logger.info("Generated year range: %s", years)
            return years

        except Exception as e:
            logger.error("Error generating year range: %s", e)
            return []

class FredDataAggregator:
    def __init__(self,
                 s3_bucket: str = os.getenv('S3_DATA_LAKE')):
        """
        Initialize the FRED data aggregator

        :param s3_bucket: S3 bucket to store the data
        """
        logger.info("Initializing FREDDataAggregator with S3 bucket: %s", s3_bucket)
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3')

        if not self.s3_bucket:
            raise ValueError("S3 bucket name is not set. Please set the S3_DATA_LAKE environment variable.")

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

    def save_parquet_to_s3(self, df: pd.DataFrame, s3_path: str) -> None:
        """
        Save a DataFrame as a Parquet file to S3

        :param df: DataFrame to save
        :param s3_path: S3 path to save the Parquet file
        """
        logger.info("Saving Parquet file to S3: %s", s3_path)
        try:
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            self.s3_client.put_object(Bucket=self.s3_bucket, Key=s3_path, Body=buffer.getvalue())
            logger.info("Saved Parquet file to S3 successfully")
        except Exception as e:
            logger.error("Error saving Parquet file to S3: %s", e)

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
        years = YearRangeGenerator.generate_years(start_year, end_year)
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

if __name__ == '__main__':
    result = aggregate_fred_indicator_processed_data(
        series_id='UNRATE',
        start_year=2016,
        end_year=2016
    )
    if result:
        logger.info("Aggregation completed successfully. S3 paths: %s", result)
        for path in result:
            print(path)
    else:
        logger.error("Aggregation failed. No data returned.")
