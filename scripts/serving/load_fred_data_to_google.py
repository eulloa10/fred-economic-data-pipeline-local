import logging
import os
from typing import Optional, List
import io
import pandas as pd
import gspread
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from airflow.hooks.S3_hook import S3Hook
from oauth2client.service_account import ServiceAccountCredentials

load_dotenv(os.path.join(os.path.dirname(__file__), '../../.env'))

S3_DATA_LAKE = os.getenv('S3_DATA_LAKE')
GOOGLE_CREDENTIALS = os.getenv('GOOGLE_CREDENTIALS')

logger = logging.getLogger(__name__)
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )

class FREDGoogleSheetLoader:
    def __init__(self,
                 google_sheet_name: str,
                 s3_bucket: str = S3_DATA_LAKE,
                 aws_conn_id: str = 'aws_default'):
        """
        Initializes the FREDGoogleSheetLoader.

        :param google_sheet_name: Name of the Google Sheet to write to.
        :param s3_bucket: Name of the S3 bucket containing Parquet files.
        :param aws_conn_id: Airflow connection ID for AWS.
        """
        self.s3_bucket = s3_bucket
        self.s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        self.google_sheet_name = google_sheet_name
        self.credentials_file = GOOGLE_CREDENTIALS
        if not self.s3_bucket:
            raise ValueError("S3 bucket name is not set.")
        if not self.credentials_file:
            raise ValueError("Google credentials file path is not set.")
        logger.info("FREDGoogleSheetLoader initialized with S3 bucket: %s and Google Sheet: %s", self.s3_bucket, self.google_sheet_name)

    def read_parquet_from_s3(self, s3_path: str) -> pd.DataFrame:
        """
        Reads a Parquet file from S3.

        :param s3_path: S3 path to the Parquet file.
        :return: DataFrame containing the data from the Parquet file.
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
            raise
        except Exception as e:
            logger.error("Error reading Parquet file from S3: %s", e)
            raise

    def sync_s3_to_google_sheet(self, df: pd.DataFrame, unique_columns: List[str]) -> Optional[int]:
        """
        Synchronizes a DataFrame to a Google Sheet, adding only unique rows.

        :param df: DataFrame containing the data to synchronize.
        :param unique_columns: List of column names that define a unique row.
        :return: Number of rows appended to the Google Sheet, or None on error.
        """
        try:
            if df is None or df.empty:
                logger.warning("DataFrame is empty. No data to load into Google Sheet: %s", self.google_sheet_name)
                return 0

            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials_file, scope)
            client = gspread.authorize(creds)
            sheet = client.open(self.google_sheet_name).sheet1

            all_values = sheet.get_all_values()
            if not all_values:
                logger.error("The google sheet is empty")
                raise ValueError("Google sheet is empty")

            header = all_values[0]
            data = all_values[1:]

            sheet_data = pd.DataFrame(data, columns=header)
            logger.info("Columns in sheet data: %s", sheet_data.columns)

            if not sheet_data.empty:
                existing_data = sheet_data[unique_columns]
                existing_set = set(tuple(row) for row in existing_data.values)
            else:
                existing_set = set()

            rows_to_append = []

            for _, row in df.iterrows():
                unique_row = tuple(row[unique_columns])
                if unique_row not in existing_set:
                    rows_to_append.append(row.values.tolist())

            if rows_to_append:
                try:
                    sheet.append_rows(rows_to_append)
                    logger.info("Appended %d new rows to Google Sheet: %s", len(rows_to_append), self.google_sheet_name)
                    return len(rows_to_append)
                except Exception as e:
                    logger.error("Error appending rows to Google Sheet: %s", e)
                    raise
            else:
                logger.info("No new rows to append to Google Sheet: %s", self.google_sheet_name)
                return 0

        except Exception as e:
            logger.error("Error syncing data to Google Sheet %s: %s", self.google_sheet_name, e)
            raise

    def load_from_s3_to_google_sheet(self,
                                    series_id: str,
                                    start_year: int,
                                    end_year: int,
                                    unique_columns: List[str]) -> Optional[List[str]]:
      """
        Loads data from S3 to Google Sheets.

        :param series_id: FRED series identifier.
        :param start_year: Starting year for data extraction.
        :param end_year: Ending year for data extraction.
        :param unique_columns: List of column names that define a unique row.
        :return: List of successfully loaded S3 paths.
      """
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

              rows_added = self.sync_s3_to_google_sheet(df, unique_columns)
              logger.info("Added %s rows from S3 path %s to Google Sheet: %s", rows_added, aggregated_s3_data_path, self.google_sheet_name)
              successful_s3_paths_loaded.append(aggregated_s3_data_path)

          return successful_s3_paths_loaded

      except Exception as e:
          logger.error("Error loading data from S3 to Google Sheets: %s", e)
          raise

def load_to_google_sheet(series_id: str, start_year: int, end_year: int, google_sheet_name: str) -> Optional[List[str]]:
    """
    Loads data from S3 to a Google Sheet.

    :param series_id: FRED series identifier.
    :param start_year: Starting year for data extraction.
    :param end_year: Ending year for data extraction.
    :param google_sheet_name: Name of the Google Sheet to write to.
    :return: List of successfully loaded S3 paths.
    """
    loader = FREDGoogleSheetLoader(google_sheet_name=google_sheet_name)
    unique_columns = ['indicator', 'observation_year', 'observation_month']
    return loader.load_from_s3_to_google_sheet(series_id, start_year, end_year, unique_columns)


# if __name__ == "__main__":
#     result = load_to_google_sheet(
#         series_id='UNRATE',
#         start_year=2010,
#         end_year=2010,
#         google_sheet_name='fred_data'
#     )
#     if result:
#         logger.info("Successfully loaded data to Google Sheets from the following S3 paths: %s", result)
#     else:
#         logger.error("Failed to load data to Google Sheets.")
