from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import os
from kafka import KafkaProducer
default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 12, 18, 10, 00)
}

#read json data file
def read_json_file(file_name, **kwargs):
    import logging
    base_path = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_path, file_name)
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    except Exception as e:
        logging.error(f'Error in read_json_file: {e}', exc_info=True)

companies_data = read_json_file('companies.json')
jobs_data = read_json_file('jobs.json')

#stream job data to broker browser
def stream_job_data(**kwargs):
    import json
    import time
    import logging
    from kafka import KafkaProducer

    producer = KafkaProducer( bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60: # 1 minute
            break
        try:  
            if 'jobs' in kwargs:
                listJob = kwargs['jobs']
         
            #data_length = len(listJob)  # Avoid using built-in names like 'len'
            for job in listJob:
                producer.send('jobs', json.dumps(job).encode('utf-8'))
                
            producer.flush()
        except Exception as e:
            logging.error(f'Error in stream_company_data: {e}', exc_info=True)

#stream company data to broker browser
def stream_company_data(**kwargs):
    import json
    import time
    import logging
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60: # 1 minute
            break
        try:  
            if 'companies' in kwargs:
                listCompany = kwargs['companies']
         
            #data_length = len(listCompany)
            for company in listCompany:
                producer.send('companies', json.dumps(company).encode('utf-8'))
                
            producer.flush()
        except Exception as e:
            logging.error(f'Error in stream_company_data: {e}', exc_info=True)

with DAG('crawl_job_automation',
         default_args=default_args,
         schedule_interval=timedelta(minutes= 1),
         catchup=False) as dag:
    
    read_company_file = PythonOperator(
        task_id = 'read_company_file',
        python_callable = read_json_file,
        provide_context=True,
        op_kwargs={'file_name': 'companies.json'}
    )
    
    read_job_file = PythonOperator(
        task_id = 'read_job_file',
        python_callable = read_json_file,
        provide_context=True,
        op_kwargs={'file_name': 'jobs.json'}
    )
    
    streaming_job = PythonOperator(
        task_id = 'stream_job_data',
        python_callable = stream_job_data,
        provide_context=True,
        op_kwargs={'jobs': jobs_data}
    )
    
    streaming_company = PythonOperator(
        task_id = 'stream_company_data',
        python_callable = stream_company_data,
        provide_context=True,
        op_kwargs={'companies': companies_data}
    )

[read_company_file, read_job_file] >> streaming_job >> streaming_company