import logging
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv('../../.env')

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DatabaseManager:
    def __init__(self, db_params: dict):
        self.db_params = db_params
        self.engine = self._create_engine()

    def _create_engine(self):
        try:
            db_url = f"postgresql+psycopg2://{self.db_params['user']}:{self.db_params['password']}@{self.db_params['host']}:{self.db_params['port']}/{self.db_params['database']}"
            engine = create_engine(db_url)
            logger.info("SQLAlchemy engine created successfully.")
            return engine
        except Exception as e:
            logger.error(f"Error creating SQLAlchemy engine: {e}")
            raise

    def rename_columns(self, table_name: str, column_mapping: dict):
        """
        Renames columns in a database table.

        :param table_name: Name of the table.
        :param column_mapping: Dictionary mapping old column names to new column names.
        """
        conn = self.engine.connect()
        try:
            for old_name, new_name in column_mapping.items():
                query = text(f'ALTER TABLE {table_name} RENAME COLUMN "{old_name}" TO "{new_name}"')
                conn.execute(query)
            conn.commit()
            logger.info(f"Columns in table '{table_name}' renamed successfully.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error renaming columns: {e}")
            raise
        finally:
            conn.close()

    def add_column(self, table_name: str, column_name: str, column_type: str):
        """
        Adds a new column to a database table.

        :param table_name: Name of the table.
        :param column_name: Name of the new column.
        :param column_type: Data type of the new column (e.g., 'VARCHAR', 'INTEGER', 'DATE').
        """
        conn = self.engine.connect()
        try:
            query = text(f'ALTER TABLE {table_name} ADD COLUMN "{column_name}" {column_type}')
            conn.execute(query)
            conn.commit()
            logger.info(f"Column '{column_name}' added to table '{table_name}' successfully.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error adding column: {e}")
            raise
        finally:
            conn.close()

    def create_table(self, table_name: str, columns: dict):
        """
        Creates a new table in the database.

        :param table_name: Name of the table to create.
        :param columns: Dictionary of column names and their data types (e.g., {'id': 'INT PRIMARY KEY', 'name': 'VARCHAR(255)'}).
        """
        conn = self.engine.connect()
        try:
            column_defs = ", ".join([f'"{col_name}" {col_type}' for col_name, col_type in columns.items()])
            query = text(f'CREATE TABLE {table_name} ({column_defs})')
            conn.execute(query)
            conn.commit()
            logger.info(f"Table '{table_name}' created successfully.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error creating table: {e}")
            raise
        finally:
            conn.close()

    def clear_table(self, table_name: str):
        """
        Clears all records from a database table.

        :param table_name: Name of the table to clear.
        """
        conn = self.engine.connect()
        try:
            query = text(f'DELETE FROM {table_name}')
            conn.execute(query)
            conn.commit()
            logger.info(f"All records cleared from table '{table_name}'.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error clearing table: {e}")
            raise
        finally:
            conn.close()

if __name__ == "__main__":
    db_params = {
        'host': os.getenv('RDS_HOST'),
        'database': os.getenv('RDS_DATABASE'),
        'user': os.getenv('RDS_USER'),
        'password': os.getenv('RDS_PASSWORD'),
        'port': os.getenv('RDS_PORT')
    }
    manager = DatabaseManager(db_params)

    table_name = "economic_indicators"
    column_mapping = {
        "aggregation_timestamp": "aggregated_at",
        "load_timestamp": "ingested_at",
    }
    new_column_name = "processed_at"
    new_column_type = "TIMESTAMP"

    try:
        # manager.rename_columns(table_name, column_mapping)
        # manager.add_column(table_name, new_column_name, new_column_type)
        # manager.clear_table(table_name)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
