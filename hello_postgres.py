from datetime import datetime
import csv
import os 


from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


def insert_csv_into_postgres():
    sql_statement = ""
    csv_input_filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)),'csv','deniro.csv')
    table_name = "movies"
    with open(csv_input_filepath) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
        for row in csv_reader:
            insert = f'INSERT INTO {table_name}(' + ", ".join(row.keys()) + ") VALUES " +"('"+ "', '".join(row.values()) +"');\n"
            sql_statement += insert
    
    ps_hook = PostgresHook()
    connection = ps_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    #cursor.commit()
    #sql_output_filepath = '/opt/airflow/dags/deniro.sql'
    #with open(sql_output_filepath,'w') as sql_file:
    #    sql_file.writelines(sql_statement)
    #ti.xcom_push(key='sql_load_table', value=sql_statement)


def print_postgres_table():
    request = "SELECT * FROM movies:"
    ps_hook = PostgresHook()
    connection = ps_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    response = cursor.fetchall()
    for row in response:
        print(row)


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

    insert_csv_into_postgres_task = PythonOperator(
        task_id='insert_csv_into_postgres',
        python_callable=insert_csv_into_postgres,
    )

    print_postgres_table_task = PythonOperator(
        task_id='print_postgres_table',
        python_callable=print_postgres_table,
    )

    #populate_movies_table_task = PostgresOperator(
    #    task_id="populate_movies_tables",
    #    postgres_conn_id="postgres_default",
    #    sql="sql/deniro/sql"
    #)

    create_movies_table_task >> insert_csv_into_postgres_task  >> print_postgres_table_task
    #>> populate_movies_table_task