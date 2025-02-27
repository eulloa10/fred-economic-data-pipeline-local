import logging
import os
from typing import Optional, List, Tuple

import time
import boto3
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv('../../.env')

FRED_API_KEY = os.getenv('FRED_API_KEY')
S3_DATA_LAKE = os.getenv('S3_DATA_LAKE')

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DateRangeGenerator:
    @staticmethod
    def generate_date_ranges(
        start_date: str,
        end_date: str,
    ) -> List[Tuple[str, str]]:
        """
        Generate date ranges for extraction

        :param start_date: Overall start date
        :param end_date: Overall end date
        :return: List of (start, end) date tuples
        """
        try:
            start_date = pd.to_datetime(start_date)
            end_date = pd.to_datetime(end_date)

            date_ranges = []
            current_start = start_date

            while current_start <= end_date:
                current_end = min(current_start + pd.DateOffset(months=1) - pd.DateOffset(days=1), end_date)
                date_ranges.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))
                current_start = current_end + pd.DateOffset(days=1)

            return date_ranges

        except Exception as e:
            logger.error("Error generating date ranges: %s", e)
            return []


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
                logger.warning("No observations found for series_id: %s", series_id)
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
            formatted_data['observation_month'] = date_series.dt.month.astype(str)
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
                          end_date: str) -> Optional[List[str]]:
        """
        Method to extract, format, and save FRED data to S3

        :param series_id: FRED series ID
        :param start_date: Start date for observations
        :param end_date: End date for observations
        :return: S3 path where the data is saved
        """
        date_ranges = DateRangeGenerator.generate_date_ranges(
            start_date,
            end_date
        )

        s3_paths = []

        try:
            for range_start, range_end in date_ranges:
                logger.info("Processing date range: %s to %s", range_start, range_end)

                raw_data = self.extract_fred_data(
                    series_id,
                    observation_start=range_start,
                    observation_end=range_end
                )

                if raw_data is None or raw_data.empty:
                    logger.warning("No data extracted for %s from %s to %s", series_id, range_start, range_end)
                    continue

                formatted_data = self.format_fred_data(raw_data, series_id)

                if formatted_data is None:
                    logger.warning("Data formatting failed for %s from %s to %s", series_id, range_start, range_end)
                    continue

                s3_path = self.save_to_s3(formatted_data, series_id)

                if s3_path:
                    s3_paths.append(s3_path)
                else:
                    logger.warning("Failed to save data for %s from %s to %s", series_id, range_start, range_end)

                time.sleep(5)

            return s3_paths

        except Exception as e:
            logger.error("A processing error occurred: %s", e, exc_info=True)
            return None

def extract_fred_indicator(
    series_id: str,
    start_date: str,
    end_date: str,
) -> Optional[List[str]]:
    """
    Extract, format, and save FRED raw data to S3

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
        start_date='2025-01-01',
        end_date='2025-12-31'
    )
    if result:
        print("Extraction successful. S3 Paths:")
        for path in result:
            print(path)
    else:
        print("Extraction failed or no data found")
