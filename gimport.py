import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import sqlite3

class DataImporter:
    def __init__(self, excel_file_path, sheet_name, database_file):
        self.excel_file_path = excel_file_path
        self.sheet_name = sheet_name
        self.database_file = database_file

    def clean_column_headers(self, df):
        df.columns = (
            df.columns.str.strip()
            .str.replace(' ', '_')
            .str.replace('[^a-zA-Z0-9_]', '')
            .str.lower()
        )
        return df

    def handle_missing_data(self, df):
        # Fill missing data automatically based on column data types
        df = df.apply(lambda col: col.fillna(0) if col.dtype.kind in 'biufc' else col.fillna(''))
        return df

    def import_data_into_database(self):
        # Load Excel data into a DataFrame and let pandas infer data types
        df = pd.read_excel(self.excel_file_path, sheet_name=self.sheet_name)

        # Clean column headers
        df = self.clean_column_headers(df)

        # Handle missing data
        df = self.handle_missing_data(df)

        # Get current file name and import date
        df['source_file'] = self.excel_file_path.split('/')[-1]
        df['import_date'] = datetime.now().date()

        # Create a SQLite engine
        engine = create_engine(f'sqlite:///{self.database_file}', echo=False)

        # Define the method to handle insertion with duplication check
        def my_insert(conn, table_name, keys, data_iter):
            # Extract the key columns for duplication check
            key_columns = ['source_file', 'import_date', 'smith_a', 'smith_b', 'smith_c']  # Add other key columns if needed

            # Create an SQL query to check for duplicates
            duplicate_query = f'''
                SELECT COUNT(*) FROM {table_name}
                WHERE {self._build_condition_query(key_columns)};
            '''

            # Execute the query for each row in data_iter
            for row in data_iter:
                # Check for duplicates in the database
                is_duplicate = conn.execute(duplicate_query, tuple(row[key] for key in key_columns)).scalar() > 0

                # Insert the row into the database if it's not a duplicate
                if not is_duplicate:
                    conn.execute(f"INSERT INTO {table_name} ({', '.join(keys)}) VALUES ({', '.join('?' for _ in keys)})", row)

        # Insert data into the database table, handling duplicates
        df.to_sql(name='TGMMH', con=engine, if_exists='append', index=False, method=my_insert)

    def _build_condition_query(self, columns):
        return ' AND '.join(f'{col} = ?' for col in columns)

if __name__ == '__main__':
    # Assuming 'My Exploration Data.xlsx' exists in the current directory
    excel_file_path = 'My Exploration Data.xlsx'
    sheet_name = 'Magic Data'
    database_file = 'my_database.db'

    data_importer = DataImporter(excel_file_path, sheet_name, database_file)
    data_importer.import_data_into_database()
    print("Data import into the database is complete.")
