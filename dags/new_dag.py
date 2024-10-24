import csv
import json
import os
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'email': ['shaheeralam.alam@gmail.com'],
    'email_on_failure': True,
    'email_on_success': True,
}

dag = DAG(
    dag_id='new_dag',
    default_args=default_args,
    description='A simple DAG to record temperature data daily',
    start_date= datetime(2024, 10, 23,16,15),
    schedule_interval=timedelta(minutes=5), 
    catchup=False,
)

def fetch_data(**kwargs):
    response = requests.get('https://api.weatherapi.com/v1/current.json?key=ec73f6f177744fef946112742242310&q=Karachi')
    data = response.json()   
    kwargs['ti'].xcom_push(key='my_data', value=data)

def get_temp(**kwargs):
    data = kwargs['ti'].xcom_pull(key='my_data')
    temp = data['current']['temp_c']
    feelsLike = data['current']['feelslike_c']
    time = data['location']['localtime'].split(" ")[1]
    kwargs['ti'].xcom_push(key='temp', value=temp)
    kwargs['ti'].xcom_push(key='feelsLike', value=feelsLike)
    kwargs['ti'].xcom_push(key='time', value=time)

def LoadData(**kwargs):
    temp = kwargs['ti'].xcom_pull(key='temp')
    feelsLike = kwargs['ti'].xcom_pull(key='feelsLike')
    time = kwargs['ti'].xcom_pull(key='time')
    dateToday = datetime.now().strftime("%Y-%m-%d")

    output_file = "/opt/airflow/dags/newdata.csv"

    # Check if the file exists
    file_exists = os.path.isfile(output_file)

    with open(output_file, 'a', newline='') as file:
        writer = csv.writer(file)

        # Write the header only if the file doesn't exist
        if not file_exists:
            writer.writerow(["Date", "Time", "Temp", "Feels Like"])
        
        # Append the data
        writer.writerow([dateToday, time, temp, feelsLike])
        

# Task 1
FetchData = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    provide_context=True,
    dag=dag,
)

# Task 2
GetTemperature = PythonOperator(
    task_id='get_temp',
    python_callable=get_temp,
    provide_context=True,
    dag= dag,
)


# Task 3
LoadData = PythonOperator(
    task_id='load_data',
    python_callable=LoadData,
    provide_context=True,
    dag=dag,
)

FetchData >> GetTemperature >> LoadData