import pandas as pd
import requests
import boto3
import json
import os
from dotenv import load_dotenv
import logging
from typing import Optional, List, Dict

load_dotenv('../../.env')

FRED_API_KEY = os.getenv('FRED_API_KEY')
S3_DATA_LAKE = os.getenv('S3_DATA_LAKE')

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class FREDDataExtractor:
    def __init__(self,
                 api_key: str = os.getenv('FRED_API_KEY'),
                 s3_bucket: str = os.getenv('S3_DATA_LAKE')):
        """
        Initialize the FRED data extractor

        :param api_key: FRED API key
        :param s3_bucket: S3 bucket name
        """
        self.api_key = api_key
        self.s3_bucket = s3_bucket

        if not self.api_key:
            raise ValueError("FRED API key is not set. Please set the FRED_API_KEY environment variable.")
        if not self.s3_bucket:
            raise ValueError("S3 bucket name is not set. Please set the S3_DATA_LAKE environment variable.")

    def extract_fred_data(self,
                          series_id: str,
                          observation_start: str,
                          observation_end: str) -> Optional[pd.DataFrame]:
        """
        Extract data from FRED API

        :param series_id: FRED series ID
        :param observation_start: Start date for observations
        :param observation_end: End date for observations
        :return: DataFrame with FRED data
        """

        try:
            url = (f'https://api.stlouisfed.org/fred/series/observations?'
                   f'series_id={series_id}&'
                   f'api_key={self.api_key}&'
                   f'file_type=json&'
                   f'observation_start={observation_start}&'
                   f'observation_end={observation_end}')

            logger.info("Fetching data from: %s", url)

            response = requests.get(url)
            response.raise_for_status()

            data = response.json()
            print(data)

            if 'observations' not in data:
                logger.error("No observations found for series_id: %s", series_id)
                return None

            df = pd.DataFrame(data['observations'])
            logger.info("Successfully extracted %d rows of data for series_id: %s", len(df), series_id)
            return df

        except Exception as e:
            logger.error("An unexpected error occurred: %s", e)
            return None

    def format_fred_data(self,
                         fred_raw_data: pd.DataFrame,
                         series_id: str) -> Optional[pd.DataFrame]:
        """
        Format the raw FRED data into a structured DataFrame

        :param fred_raw_data: Raw data from FRED API
        :param series_id: FRED series ID
        :return: Formatted DataFrame
        """
        try:
            if fred_raw_data is None or fred_raw_data.empty:
                logger.error("No data to format for series_id: %s", series_id)
                return None

            logger.info("Formatting data for series_id: %s", series_id)

            formatted_data = fred_raw_data.copy()

            formatted_data['indicator'] = series_id
            formatted_data['ingestion_timestamp'] = pd.Timestamp.now(tz='UTC').isoformat()

            date_series = pd.to_datetime(formatted_data['date'])
            formatted_data['observation_date'] = date_series.dt.date.astype(str)
            formatted_data['observation_month'] = date_series.dt.month.astype(str).apply(lambda x: x.zfill(2)) # zero-pad month
            formatted_data['observation_year'] = date_series.dt.year.astype(str)

            formatted_data['value'] = pd.to_numeric(formatted_data['value'], errors='coerce').astype(str)

            cols = [
                'indicator',
                'observation_date',
                'observation_month',
                'observation_year',
                'value',
                'ingestion_timestamp'
            ]

            formatted_data = formatted_data[cols]

            logger.info("Data for series_id: %s formatted successfully. Returning DataFrame with %d rows.", series_id, len(formatted_data))
            return formatted_data

        except Exception as e:
            logger.error("An unexpected error occurred during formatting for %s: %s", series_id, e)
            return None

    def save_to_s3(self,
                   df: pd.DataFrame,
                   indicator: str) -> Optional[str]:
        """
        Save the formatted DataFrame to S3 in JSON format

        :param df: Formatted DataFrame
        :param indicator: FRED series ID
        :return: JSON string of the DataFrame
        """
        try:
            if df is None or df.empty:
                logger.error("DataFrame is None or empty. Cannot save to S3.")
                return None

            year = df['observation_year'].iloc[0]
            month = df['observation_month'].iloc[0]

            json_buffer = df.to_json(orient='records', lines=True)
            json_bytes = json_buffer.encode('utf-8')

            s3_path = (f"raw_data/indicator={indicator}/"
                       f"year={year}/"
                       f"month={month}/"
                       f"{indicator}_{year}{month}_data.json")

            s3_client = boto3.client('s3')
            s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_path,
                Body=json_bytes,
                ContentType='application/json'
            )

            logger.info("Successfully saved data to %s", s3_path)
            return s3_path

        except Exception as e:
            logger.error("An unexpected error occurred during S3 save: %s", e)
            return None

    def process_fred_data(self,
                          series_id: str,
                          start_date: str,
                          end_date: str) -> Optional[str]:
        """
        Method to extract, format, and save FRED data to S3

        :param series_id: FRED series ID
        :param start_date: Start date for observations
        :param end_date: End date for observations
        :return: S3 path where the data is saved
        """
        try:
            raw_data = self.extract_fred_data(
                series_id,
                observation_start=start_date,
                observation_end=end_date
            )

            if raw_data is None:
                return None

            formatted_data = self.format_fred_data(raw_data, series_id)

            if formatted_data is None:
                return None

            s3_path = self.save_to_s3(formatted_data, series_id)

            return s3_path
        except Exception as e:
            logger.error("A processing error occured: %s", e)
            return None

def extract_fred_indicator(
    series_id: str,
    start_date: str,
    end_date: str,
) -> Optional[str]:
    """
    Airflow: Extract, format, and save FRED data to S3

    :param series_id: FRED series ID
    :param start_date: Start date for observations
    :param end_date: End date for observations
    :return: S3 path where the data is saved
    """
    extractor = FREDDataExtractor()
    return extractor.process_fred_data(series_id, start_date, end_date)

if __name__ == '__main__':
    result = extract_fred_indicator(
        series_id='UNRATE',
        start_date='2024-12-01',
        end_date='2024-12-31'
    )
    print(f"Extraction result: {result}")
