import os
import csv
import logging
import configparser
import pandas as pd
import snowflake.connector
from urllib.parse import quote

# Set up logging
logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, config_path):
        config = configparser.ConfigParser()
        config.read(config_path)

        # Configuration parameters
        self.sentinel_value = config.get('defaults', 'sentinel_value', fallback="0001-01-01 00:00:00.000")
        self.datetime_format = config.get('defaults', 'datetime_format', fallback="%Y-%m-%d %H:%M:%S.%f")

        # Snowflake credentials - securely handled
        self.snowflake_user = os.getenv('SNOWFLAKE_USER', config['snowflake']['user'])
        self.snowflake_password = os.getenv('SNOWFLAKE_PASSWORD', config['snowflake']['password'])
        self.snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT', config['snowflake']['account'])
        self.snowflake_warehouse = config['snowflake']['warehouse']
        self.snowflake_database = config['snowflake']['database']
        self.snowflake_schema = config['snowflake']['schema']
        self.snowflake_role = config['snowflake']['role']

        # Initialize Snowflake connection
        self.conn = self.create_snowflake_connection()
        self.cursor = self.conn.cursor()

        self.column_definition = ""
        self.column_context = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cursor.close()
        self.conn.close()

    def create_snowflake_connection(self):
        try:
            return snowflake.connector.connect(
                user=self.snowflake_user,
                password=self.snowflake_password,
                account=self.snowflake_account,
                warehouse=self.snowflake_warehouse,
                database=self.snowflake_database,
                schema=self.snowflake_schema,
                role=self.snowflake_role,
            )
        except Exception as e:
            logger.error("Failed to create Snowflake connection", exc_info=True)
            raise

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            return self.cursor.sfqid
        except Exception as e:
            logger.error(f"Failed to execute query: {e}", exc_info=True)
            raise

    
    def get_table_result_from_cur(self, qid):
        self.cursor.get_results_from_sfqid(qid)
        result = self.cursor.fetchall()
        columns = self.cursor.description
        return result, columns
    

    def generate_col_definitions(self, df):
        """
        Generate column definitions for the CREATE TABLE statement in Snowflake
        """
        column_definitions = []
        for column, data_type in zip(df.columns, df.dtypes):
            snowflake_data_type = 'TEXT'  # default data type
            if pd.api.types.is_integer_dtype(data_type):
                snowflake_data_type = 'NUMBER'
            elif pd.api.types.is_float_dtype(data_type):
                snowflake_data_type = 'FLOAT'
            elif pd.api.types.is_datetime64_any_dtype(data_type):
                snowflake_data_type = 'TIMESTAMP'
            elif pd.api.types.is_string_dtype(data_type):
                snowflake_data_type = 'TEXT'
            column_definitions.append(f'"{column}" {snowflake_data_type}')

        column_definitions_str = ', '.join(column_definitions)
        logger.info("Column definitions successfully generated")
        return column_definitions_str

    def delete_folder_contents(self, folder_path):
        """
        Recursively delete the contents of a folder.
        """
        try:
            for file_name in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file_name)
                if os.path.isdir(file_path):
                    self.delete_folder_contents(file_path)
                    os.rmdir(file_path)
                else:
                    os.remove(file_path)
            logger.info(f"Folder content of: {folder_path} deleted!")
        except Exception as e:
            logger.error(f"Error while deleting folder contents: {e}", exc_info=True)
            raise

    def local_stage_df(self, df, file_path):
        """
        Preprocess the DataFrame and save it as a CSV file
        """
        try:
            # Remove any special characters from the DataFrame
            df.replace(to_replace=[r"\\t|\\n|\\r", "\\t|\\n|\\r",'"'], value=["","",""], regex=True, inplace=True)
        except ValueError:
            df.replace("\\n", "", inplace=True)
        except Exception as e:
            logger.error(f"Error while replacing special characters: {e}")
            raise

        # Replace all numeric null values with 'NULL'
        for col in df.columns:
            # check if the column contains numeric values
            if pd.api.types.is_numeric_dtype(df[col]):
                # replace empty values with 'NULL'
                df[col].fillna('NULL', inplace=True)

        # Export the DataFrame to a CSV file
        try:
            df.to_csv(
                file_path,
                index=False,
                sep='~',
                encoding='utf-8',
                na_rep='NULL',  # Replace missing values with 'NULL'
                quoting=csv.QUOTE_NONNUMERIC,  # Quote all non-numeric values
                quotechar='"',  # Use double quotes as the quoting character
                lineterminator='\n'
            )
            logger.info(f"Cleaned data successfully exported to CSV format at {file_path}")
            return True
        except Exception as e:
            logger.error(f"Error while saving DataFrame to CSV: {e}")
            return False

    def clean(self, df):
        """
        Clean the DataFrame: replace 'NaT' values, convert datetime columns to strings,
        and convert columns with more than one type to string
        """

        # Find columns containing 'NaT' values
        columns_with_nat = []
        for column, data_type in df.dtypes.items():
            if data_type in ['datetime64[ns]', '<M8[ns]']:
                # Check if the column contains 'NaT' values
                if (df[column].isnull() & (df[column].notnull() == False)).any():
                    columns_with_nat.append(column)

        # Replace 'NaT' values with None and convert datetime columns to strings
        for column, data_type in df.dtypes.items():
            if str(data_type) in ['datetime64[ns]', '<M8[ns]']:
                df[column] = df[column].apply(lambda x: x.strftime(self.datetime_format) if pd.notnull(x) else self.sentinel_value)

        # Convert any column with more than one type to string
        for column in df.columns:
            if df[column].apply(type).nunique() > 1:
                df[column] = df[column].astype(str)

        return df

    def has_csv_files(self, folder_path):
            """
            Check if the given folder contains any CSV files
            """
            for file_name in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file_name)
                # Check if the file is a CSV file
                if file_name.endswith('.csv') and os.path.isfile(file_path):
                    return True

            # No CSV files were found
            return False

    def log_column_mismatch(self, df, file_name):
        """
        Log the column names if they match with the reference DataFrame.
        If there is a mismatch, log the details of the CSV file.
        """
        # Get the column names of the DataFrame and the reference DataFrame
        if self.column_context == None:
            self.column_context = set(df.columns)
            return None
        else:
            df_columns = set(df.columns)
            reference_columns = self.column_context

            if df_columns == reference_columns:
                logger.info("Column names match the reference DataFrame")
            else:
                extra_columns = df_columns - reference_columns
                missing_columns = reference_columns - df_columns

                if extra_columns:
                    reference_columns = reference_columns.update(extra_columns)
                    logger.warning(f"Extra columns in file {file_name}: {', '.join(extra_columns)}")
                if missing_columns:
                    logger.warning(f"Missing columns in file {file_name}: {', '.join(missing_columns)}")
                
                self.column_context = reference_columns
            return None

    def process_flat_files(self, input_location, staging_location):
        """
        Process Excel files: read the files, clean the data, and save it as CSV files
        """
        self.staging_location = staging_location
        for file_name in os.listdir(input_location):
            file_path = os.path.join(input_location, file_name)
            try:
                if (file_name.endswith('.xlsx') or file_name.endswith('.XLSX') or file_name.endswith('.xls')) and os.path.isfile(file_path):
                    # Read the Excel file
                    df = pd.read_excel(file_path)
                elif file_name.endswith('.csv') and os.path.isfile(file_path):
                    df = pd.read_csv(file_path)
            except Exception as e:
                logger.error(f"Error while reading the files in the input folder: {e}")
            # Log Column mismatch if any
            self.log_column_mismatch(df, file_name)
            # Clean the DataFrame
            df = self.clean(df)
            # Add Column for file identifier
            df['File Name'] = file_name
            # Save the DataFrame as a CSV file
            stage_file_path = os.path.join(staging_location, f'{os.path.splitext(file_name)[0]}.csv')
            self.local_stage_df(df, stage_file_path)

        self.column_definition = self.generate_col_definitions(df)

        return None


    def load_staged_files_to_snowflake(self, name):
        """
        Load CSV files to Snowflake: generate column definitions, create the table,
        and copy the data from the CSV files to the table
        """
        table_name = f'{name}_TABLE'
        stage_name = f'{name}_STAGE'

        staging_location = self.staging_location

        if self.has_csv_files(folder_path=staging_location):

            col_def_str = self.column_definition
            create_table_query = f""" CREATE OR REPLACE TABLE {self.snowflake_database+'.'+self.snowflake_schema+'.'+table_name} (
                {col_def_str});"""
            self.execute_query(create_table_query)
                
            internal_stage_handling = f"""CREATE OR REPLACE STAGE {self.snowflake_database+'.'+self.snowflake_schema+'.'+stage_name}"""
            self.execute_query(internal_stage_handling)

            # Upload the Parquet file to the Snowflake internal stage
            logging.info(f"Staging location {staging_location}")

            formatted_staging_location = staging_location.replace('\\', '/')
            if ' ' in formatted_staging_location:
                formatted_staging_location = f"PUT 'file://{formatted_staging_location}/*.csv'"
                put_command = f"{formatted_staging_location} @{stage_name};" 
            else: 
                put_command = f"PUT file://{formatted_staging_location}/*.csv @{stage_name};"

            print(put_command)

            put_qid = self.execute_query(put_command)

            # Create or replace file format
            file_format_handling = '''
                CREATE OR REPLACE FILE FORMAT CSV_FF_MANY_NULL 
                TYPE = 'CSV' FIELD_DELIMITER = '~' RECORD_DELIMITER = '\\n' 
                SKIP_HEADER = 0 FIELD_OPTIONALLY_ENCLOSED_BY = '\\042' 
                NULL_IF = ('\\\\N', 'Null', 'NULL', 'null', '\\\\n', 'nan') 
                ESCAPE_UNENCLOSED_FIELD = '\\\\' ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE 
                PARSE_HEADER = TRUE
                '''
            self.execute_query(file_format_handling)
            
            # Copy into table
            copy_command = f'''COPY INTO {table_name} 
                    FROM @{stage_name} 
                    FILE_FORMAT = csv_ff_many_null
                    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';
                    '''
            copy_qid = self.execute_query(copy_command)

            self.delete_folder_contents(folder_path=staging_location)
                        
            return True, put_qid, copy_qid
        else:
            raise FileNotFoundError('Specified folder does not have any CSV files staged')
        

    def main_load(self, name, input_location, staging_location):
            """
            Main function to load data: process Excel files and load CSV files to Snowflake
            """
            try:
                # Process Excel files
                self.process_flat_files(input_location=input_location,staging_location=staging_location)
                print("Excel files processed successfully.")
                
                # Check if there are CSV files to load
                if not self.has_csv_files(staging_location):
                    print("No CSV files found to load. Please check the preprocessing logs")
                    return None
                
                # Load CSV files to Snowflake
                success, pid, cid = self.load_staged_files_to_snowflake(name)
                if success:
                    print(f"CSV files loaded to Snowflake successfully \nPut ID: {pid} \nCopy ID: {cid}")

            except Exception as e:
                print(f"Error in main_load: {str(e)}", type='error')

