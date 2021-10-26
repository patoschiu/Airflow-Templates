from datetime import datetime
import csv
import os 
import io


from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd


def create_sql_statement(ti):
    sql_statement = ""
    csv_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),'csv','deniro.csv')
    table_name = "movies"
    with open(csv_path) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
        for row in csv_reader:
            insert = f'INSERT INTO {table_name}(' + ", ".join(row.keys()) + ") VALUES " +"('"+ "', '".join(row.values()) +"');\n"
            sql_statement += insert
    ti.xcom_push(key='sql_load_table', value=sql_statement)


def postgres_to_s3():
    request = "SELECT * FROM movies;"
    ps_hook = PostgresHook()
    connection = ps_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    response = cursor.fetchall()
    df = pd.DataFrame(response, columns=['Year', 'Score', 'Title'])
    print(df.to_markdown()) 
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    #s3_hook.create_bucket(bucket_name="s3://postgres-parquet-export-test")
    with io.BytesIO() as buffer:                                
        df.to_parquet(buffer, engine='auto')
        s3_hook.load_file_obj(buffer, key='movies.parquet', bucket_name='postgres-parquet-export')


with DAG(
    dag_id="hello_postgres",
    start_date=datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
) as dag:
    create_movies_table_task = PostgresOperator(
        task_id="create_movies_table",
        postgres_conn_id="postgres_default",
        sql="""
            DROP TABLE movies;
            CREATE TABLE IF NOT EXISTS movies (
            Year VARCHAR NOT NULL,
            Score VARCHAR NOT NULL,
            Title VARCHAR NOT NULL);
        """
    )

    create_sql_file_task = PythonOperator(
        task_id='create_sql_statement',
        python_callable=create_sql_statement,
    )

    populate_movies_table_task = PostgresOperator(
        task_id="populate_movies_tables",
        postgres_conn_id="postgres_default",
        sql="{{ ti.xcom_pull(key='sql_load_table',task_ids='create_sql_statement') }}"
    )

    postgres_to_s3_task = PythonOperator(
        task_id='postgres_to_s3',
        python_callable=postgres_to_s3,
    )

    create_movies_table_task >> create_sql_file_task >> populate_movies_table_task >> postgres_to_s3_task