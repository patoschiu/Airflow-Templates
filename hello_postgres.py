from datetime import datetime
import csv
import os 


from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator


def create_sql_statement(ti):
    sql_statement = ""
    csv_input_filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)),'csv','deniro.csv')
    table_name = "movies"
    with open(csv_input_filepath) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
        for row in csv_reader:
            insert = f'INSERT INTO {table_name}(' + ", ".join(row.keys()) + ") VALUES " +"('"+ "', '".join(row.values()) +"');\n"
            sql_statement += insert
    
    sql_output_filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)),'sql','deniro.sql')
    with open(sql_output_filepath,'w') as sql_file:
        sql_file.writelines(sql_statement)
    #ti.xcom_push(key='sql_load_table', value=sql_statement)


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
        sql="sql/deniro/sql"
    )

    create_movies_table_task >> create_sql_file_task >> populate_movies_table_task