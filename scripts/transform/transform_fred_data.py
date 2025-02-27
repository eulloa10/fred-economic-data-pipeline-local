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

class YearMonthGenerator:
    @staticmethod
    def generate_year_months(start_date: str,
                             end_date: str
                            ) -> Dict[str, List[str]]:
        """
        Generate year and month combinations for a given date range

        :param start_date: Overall start date
        :param end_date: Overall end date
        :return: Dictionary with year as key and list of months as value
        """
        logger.info("Generating year-month combinations from %s to %s", start_date, end_date)

        try:
            start_date = pd.to_datetime(start_date)
            end_date = pd.to_datetime(end_date)

            year_months = {}
            current_date = start_date
            while current_date <= end_date:
                year = current_date.year
                month = current_date.month
                if year not in year_months:
                    year_months[year] = []
                if month not in year_months[year]:
                    year_months[year].append(month)
                current_date += pd.DateOffset(months=1)

            logger.info("Generated year-month combinations: %s", year_months)
            return year_months

        except Exception as e:
            logger.error("Error generating year-month combinations: %s", e)
            return {}

class FREDDataProcessor:
    def __init__(self,
                 s3_bucket: str = os.getenv('S3_DATA_LAKE')):
        """
        Initialize the FRED data processor

        :param api_key: FRED API key
        :param s3_bucket: S3 bucket for the data lake
        """
        logger.info("Initializing FREDDataProcessor with S3 bucket: %s", s3_bucket)
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3')

        if not self.s3_bucket:
            raise ValueError("S3 bucket name is not set. Please set the S3_DATA_LAKE environment variable.")

    def read_json_from_s3(self, s3_path: str) -> pd.DataFrame:
        """
        Read JSON data from S3

        :param s3_path: S3 path to the JSON file
        :return: DataFrame containing the JSON data
        """
        logger.info("Reading JSON from S3 path: %s", s3_path)

        try:
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_path)
            raw_data = response['Body'].read().decode('utf-8')
            logger.debug("Raw data read from S3: %s", raw_data[:10])

            try:
                data = pd.read_json(io.StringIO(raw_data), lines=True)
                logger.info("Successfully read JSON data from %s", s3_path)
                return data
            except ValueError:
                logger.warning("Failed to read JSON with lines=True, trying without it.")
                data = pd.read_json(io.StringIO(raw_data))
                logger.info("Successfully read JSON data from %s without lines=True", s3_path)
                return data

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning("File not found at %s. Returning empty DataFrame.", s3_path)
                return pd.DataFrame()
            else:
                logger.error("Error reading JSON from S3: %s", e)
                return pd.DataFrame()
        except Exception as e:
            logger.error("Error reading JSON from S3: %s", e)
            return pd.DataFrame()

    def transform_raw_data(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw data by cleaning and aggregating
        :param raw_data: DataFrame containing the raw data
        :return: Transformed DataFrame
        """
        logger.info("Transforming raw data with %d rows", len(raw_data))

        try:
            if raw_data.empty:
                logger.warning("No raw data available to transform.")
                return pd.DataFrame()

            logger.info("Dropping rows with NaN values in 'value' column")
            raw_data = raw_data.dropna(subset=['value'])
            raw_data = raw_data[raw_data['value'] != '.']

            logger.info("Converting 'value' column to numeric")
            raw_data['value'] = pd.to_numeric(raw_data['value'], errors='coerce')

            logger.info("Grouping data by 'indicator' and aggregating")
            raw_data = raw_data.groupby(['indicator', 'observation_month', 'observation_year'], as_index=False).agg(
                value=('value', 'mean'),
                observation_count=('value', 'count'),
                ingested_at=('ingested_at', 'max')
            )

            logger.info("Converting ingest timestamp from UNIX to ISO format")
            raw_data['ingested_at'] = pd.to_datetime(raw_data['ingested_at'], unit='ms', utc=True).dt.tz_convert('UTC').dt.strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')


            logger.info("Adding processing timestamp")
            raw_data['processed_at'] = pd.Timestamp.now(tz='UTC').isoformat()

            cols = [
              'indicator', 'observation_year', 'observation_month', 'value', 'observation_count', 'ingested_at', 'processed_at'
            ]

            transformed_data = raw_data[cols]

            logger.info("Transformed complete with %d rows", len(transformed_data))
            return transformed_data

        except Exception as e:
            logger.error("Error transforming raw data: %s", e)
            return pd.DataFrame()

    def save_parquet_to_s3(self,
                           df: pd.DataFrame,
                           s3_path: str) -> None:
        """
        Save DataFrame to S3 in Parquet format

        :param df: DataFrame to save
        :param s3_path: S3 path to save the Parquet file
        """
        logger.info("Saving DataFrame to S3 path: %s", s3_path)

        try:
            if df.empty:
                logger.warning("DataFrame is empty. No data to save.")
                return

            buffer = df.to_parquet(index=False)
            self.s3_client.put_object(Bucket=self.s3_bucket, Key=s3_path, Body=buffer)
            logger.info("Data saved to S3 at %s", s3_path)

        except Exception as e:
            logger.error("Error saving Parquet to S3: %s", e)

    def process_raw_data(self,
                     series_id: str,
                     start_date: str,
                     end_date: str
                    ) -> Optional[List[str]]:
        """
        Process the raw data for a given indicator, year, and month
        :param series_id: FRED series ID
        :param start_date: Overall start date
        :param end_date: Overall end date
        :return: Processed data as a list of strings
        """
        logger.info("Processing raw data for series_id: %s, start_date: %s, end_date: %s", series_id, start_date, end_date)
        year_month_ranges = YearMonthGenerator.generate_year_months(
            start_date,
            end_date
        )

        s3_paths = []

        try:
            for year, months in year_month_ranges.items():
                logger.info("Processing year: %d with months: %s", year, months)
                for month in months:
                    raw_data_path = f'raw_data/indicator={series_id}/year={year}/month={month}/{series_id}_{year}_{month}.json'
                    logger.info(f"Reading raw data from {raw_data_path}")
                    raw_data = self.read_json_from_s3(raw_data_path)

                    processed_data = self.transform_raw_data(raw_data)

                    processed_data_path = f'processed_data/indicator={series_id}/year={year}/month={month}/{series_id}_{year}_{month}.parquet'
                    logger.info(f"Saving processed data to {processed_data_path}")
                    self.save_parquet_to_s3(processed_data, processed_data_path)
                    s3_paths.append(processed_data_path)

            logger.info("All data processed and saved to S3")
            return s3_paths

        except Exception as e:
            logger.error("Error processing raw data: %s", e)
            return None

def transform_fred_indicator_raw_data(
    series_id: str,
    start_date: str,
    end_date: str) -> Optional[List[str]]:
    """
    Transform FRED indicator raw data
    :param indicator: FRED indicator
    :param start_date: Overall start date
    :param end_date: Overall end date
    :return: Processed data as a list of strings
    """
    logger.info("Transforming FRED indicator raw data for series_id: %s, start_date: %s, end_date: %s", series_id, start_date, end_date)
    processor = FREDDataProcessor()
    return processor.process_raw_data(series_id, start_date, end_date)

if __name__ == '__main__':
    result = transform_fred_indicator_raw_data(
        series_id='UNRATE',
        start_date='2016-01-01',
        end_date='2016-01-31'
    )
    if result:
        logger.info("Transform successful. Processed data paths:")
        for path in result:
            print(path)
    else:
        logger.error("Transform failed. No data processed.")
