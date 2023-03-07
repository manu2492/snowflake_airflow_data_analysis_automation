from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from airflow.operators.dummy_operator import DummyOperator

"""
DAG to generate Snowflake tables, with three steps to  insert into a table. 
A notification is sent via Slack once all three tables are available. 

Args:
    owner (str): Owner of the DAG
    depends_on_past (bool): Whether past DAG runs need to complete before this DAG can start
    start_date (datetime): The start date of the DAG
    retries (int): The number of retries that should be attempted before the task fails.

Returns:
    None
"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 5),
    'retries': 3,
}

dag = DAG('snowflake_tables_5', default_args=default_args, schedule_interval=timedelta(minutes=15))


def generate_table_name(prefix):
    """
    Generate table name by concatenating prefix with timestamp

    Args:
        prefix (str): Prefix for table name

    Returns:
        str: Generated table name
    """
    timestamp = datetime.now().strftime('%Y%m%d%H%M')
    return f"{prefix}_{timestamp}"


def send_slack_notification(message):
    """
    Send Slack notification

    Args:
        message (str): Message to send in the Slack notification

    Returns:
        None
    """
    client = WebClient(token='vV3iRJ2odmy3R1psk82TA4Is')
    channel_name = 'data'
    try:
        # Call the chat.postMessage method using the WebClient
        response = client.chat_postMessage(
            channel=channel_name,
            text=message
        )
    except SlackApiError as e:
        print("Error sending message: {}".format(e))


with dag:

    create_table_step1 = SnowflakeOperator(
        task_id='create_table_step1',
        sql=f"CREATE TABLE {generate_table_name('members_usuarios_ciudad_aux1')} AS SELECT city, member_id, MIN(joined) AS joined FROM members m GROUP BY 1, 2;",
        snowflake_conn_id='snowflake',
    )

    create_table_step2 = SnowflakeOperator(
        task_id='create_table_step2',
        sql=f"CREATE TABLE {generate_table_name('members_usuarios_ciudad_aux2')} AS SELECT city, member_id AS id, joined, EXTRACT(YEAR FROM CAST(joined AS DATE)) AS anio, EXTRACT(MONTH FROM CAST(joined AS DATE)) AS mes FROM {generate_table_name('members_usuarios_ciudad_aux1')};",
        snowflake_conn_id='snowflake',
    )

    create_table_step3 = SnowflakeOperator(
        task_id='create_table_step3',
        sql=f"CREATE TABLE {generate_table_name('members_usuarios_ciudad_aux3')} (city VARCHAR(50), anio INT, num_members INT);",
        snowflake_conn_id='snowflake',
    )

    insert_data_step3 = SnowflakeOperator(
        task_id='insert_data_step3',
        sql=f"INSERT INTO {generate_table_name('members_usuarios_ciudad_aux3')} (city, anio, num_members) SELECT city, anio, COUNT(id) FROM {generate_table_name('members_usuarios_ciudad_aux2')} GROUP BY 1, 2 ORDER BY 1, 2;",
        snowflake_conn_id='snowflake',
    )

    notify_create_table_step3 = PythonOperator(
        task_id='notify_create_table_step3',
        python_callable=send_slack_notification,
        op_kwargs={'message': 'All three tables are available.'},
    )

    dag_complete = DummyOperator(
        task_id='dag_complete',
    )

    create_table_step1 >> create_table_step2 >> create_table_step3 >> insert_data_step3 >> notify_create_table_step3 >> dag_complete
