import snowflake.connector


def connect_to_snowflake(user: str, password: str, account: str, database: str, schema: str) -> object:
    """Create a connection with Snowflake and return a connection object"""

    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            database=database,
            schema=schema
        )
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {str(e)}")
        return None


def create_stage(conn: object, stage_name: str) -> None:
    """Create a stage in Snowflake"""

    try:
        cur = conn.cursor()
        cur.execute(f'CREATE OR REPLACE STAGE {stage_name}')
    except Exception as e:
        print(f"Error creating stage {stage_name}: {str(e)}")


def upload_csv_to_stage(conn: object, stage_name: str, file_path: str) -> None:
    """Load a CSV file to a Snowflake stage"""

    try:
        with open(file_path, 'rb') as file:
            cur = conn.cursor()
            cur.execute(f"PUT file://{file.name} @{stage_name}")
    except Exception as e:
        print(f"Error uploading file {file_path} to stage {stage_name}: {str(e)}")


def copy_data_from_stage_to_table(conn: object, table_name: str, stage_name: str, csv_format: str) -> None:
    """Copy data from a Snowflake stage to a table"""

    try:
        cur = conn.cursor()
        cur.execute(f"COPY INTO {table_name} FROM @{stage_name} FILE_FORMAT=(TYPE=CSV {csv_format})")
    except Exception as e:
        print(f"Error copying data from stage {stage_name} to table {table_name}: {str(e)}")

# Establish a connection to Snowflake
conn = connect_to_snowflake(user='your_user', password='your_password', account='your_account', database='RAPPI', schema='PUBLIC')

# Create a stage
create_stage(conn, stage_name='my_stage')

# Upload the CSV file to the stage
upload_csv_to_stage(conn, stage_name='my_stage', file_path='./archive/members.csv')

# Copy the data from the stage to the table
copy_data_from_stage_to_table(conn, table_name='members', stage_name='my_stage', csv_format="FIELD_DELIMITER=',' FIELD_OPTIONALLY_ENCLOSED_BY='\"' SKIP_HEADER=1 error_on_column_count_mismatch=false ON_ERROR='SKIP_FILE'")
