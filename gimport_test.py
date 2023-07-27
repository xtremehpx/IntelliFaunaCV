import unittest
import pandas as pd
import os
import sqlite3
from gimport import DataImporter

# python -m unittest gimport_test.py
class TestDataImporter(unittest.TestCase):
    def setUp(self):
        # Sample data for testing with missing values
        data = {
            'A': [1, None, 3, 4, 5],
            'B': [10.5, 20.5, None, 40.5, 50.5],
            'Smith A': ['John', 'Jane', None, 'John', 'Jane'],
            'Smith B': ['Doe', None, 'Smith', 'Doe', 'Doe'],
            'Smith C': ['X', 'Y', 'Z', 'X', None]
        }
        self.df = pd.DataFrame(data)

        # Save sample data to a temporary Excel file for testing
        self.excel_file_path = 'test_data.xlsx'
        self.df.to_excel(self.excel_file_path, index=False)

        # Create a test database file
        self.database_file = 'test_database.db'

    def test_handle_missing_data(self):
        # Initialize the DataImporter
        data_importer = DataImporter(self.excel_file_path, 'Sheet1', self.database_file)

        # Load Excel data into a DataFrame
        df = pd.read_excel(self.excel_file_path, sheet_name='Sheet1')

        # Handle missing data
        df_handled = data_importer.handle_missing_data(df)

        # Check if missing values are handled correctly
        self.assertTrue(df_handled.isna().sum().sum() == 0)

    def test_duplication_check(self):
        # Initialize the DataImporter
        data_importer = DataImporter(self.excel_file_path, 'Sheet1', self.database_file)

        # Load Excel data into a DataFrame
        df = pd.read_excel(self.excel_file_path, sheet_name='Sheet1')

        # Ensure there are no duplicates initially
        conn = sqlite3.connect(self.database_file)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM TGMMH")
        initial_count = cursor.fetchone()[0]
        conn.close()
        self.assertEqual(initial_count, 0)

        # Import data into the database
        data_importer.import_data_into_database()

        # Check if duplicates are handled correctly during import
        conn = sqlite3.connect(self.database_file)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM TGMMH")
        count_after_import = cursor.fetchone()[0]
        conn.close()
        self.assertEqual(count_after_import, 3)  # Expects 3 unique records after import

    def test_data_read_into_dataframe(self):
        # Initialize the DataImporter
        data_importer = DataImporter(self.excel_file_path, 'Sheet1', self.database_file)

        # Load Excel data into a DataFrame
        df = pd.read_excel(self.excel_file_path, sheet_name='Sheet1')

        # Ensure data is read properly into the DataFrame
        self.assertTrue(df.equals(self.df))

    def test_database_table_creation(self):
        # Initialize the DataImporter
        data_importer = DataImporter(self.excel_file_path, 'Sheet1', self.database_file)

        # Import data into the database
        data_importer.import_data_into_database()

        # Check if the database table 'TGMMH' is created
        conn = sqlite3.connect(self.database_file)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='TGMMH';")
        table_exists = cursor.fetchone()
        conn.close()
        self.assertIsNotNone(table_exists)

    def test_data_import_into_database(self):
        # Initialize the DataImporter
        data_importer = DataImporter(self.excel_file_path, 'Sheet1', self.database_file)

        # Import data into the database
        data_importer.import_data_into_database()

        # Check if data is imported properly into the database
        conn = sqlite3.connect(self.database_file)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM TGMMH")
        count_after_import = cursor.fetchone()[0]
        conn.close()
        self.assertEqual(count_after_import, 3)  # Expects 3 unique records after import

    def tearDown(self):
        # Remove the temporary Excel file and test database after the test
        if os.path.exists(self.excel_file_path):
            os.remove(self.excel_file_path)

        if os.path.exists(self.database_file):
            os.remove(self.database_file)

if __name__ == '__main__':
    unittest.main()
