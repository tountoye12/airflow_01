

from datetime import datetime, timedelta

from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd


default_args = {
    'owner': 'Diallo'
}



def load_data():
    df = pd.read_csv('./dataframes/insurance.csv')

    print("################# DataFrame #################")
    
    print("Dataset shape ==>", df.shape)
    print(df)
    

    return df.to_json()



def clean_data(**kwargs):
    ti = kwargs['ti']

    json_data = ti.xcom_pull(task_ids='read_csv_file')


    df = pd.read_json(json_data)
    print("Dataset shape ==>", df.shape)
    df = df.dropna()
    print(df)

    return df.to_json()

with DAG(
    dag_id = 'python_pipeline',
    description = 'Running a python pipeline using Airflow',
    default_args =  default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['simple', 'python', 'transform', 'pipeline']
) as dag:
    read_csv_file = PythonOperator(
        task_id = 'read_csv_file',
        python_callable = load_data
        
    )

    clean_data = PythonOperator(
        task_id = 'clean_data',
        python_callable = clean_data
        
    )

    

read_csv_file >> clean_data