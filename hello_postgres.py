from datetime import datetime
import csv
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator


def create_sql_file(csv_path, sql_path, table_name):
    with open(sql_path,'w') as sql_file:
        with open(csv_path) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
            for row in csv_reader:
                insert = f'INSERT INTO {table_name} (' + ", ".join(row.keys()) + ") VALUES " +'("'+ '", "'.join(row.values()) +'");\n'
                sql_file.write(insert)


with DAG(
    dag_id="postgres_operator_dag",
    start_date=datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
) as dag:
    create_pet_table_task = PostgresOperator(
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
        task_id='create_sql_file_task',
        python_callable=create_sql_file,
        op_kwargs={
            "csv_path":'deniro.csv',
            "sql_path":'sql/deniro.sql',
            "table_name":""
        },
    )

    populate_movies_table_task = PostgresOperator(
        task_id="populate_movies_table_task",
        postgres_conn_id="postgres_default",
        sql="sql/deniro.sql",
    )

    create_pet_table_task >> create_sql_file_task >> populate_movies_table_task