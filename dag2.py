import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from pathlib import Path
from utils import Utility 

base_dir = str(Path.home())

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': dt.datetime.strptime('2018-07-29T00:00:00', '%Y-%m-%dT%H:%M:%S'),
    'provide_context': True
}
# Instantiate the DAG
dag = DAG('dag2', default_args=default_args,
          schedule_interval='0 0 * * *', max_active_runs=1)# scheduled to run everyday at midnight


util=Utility(news_api_key='',s3_bucket='')

# fetch headlines for a string of keyword
def getHeadlines(**kwargs):
    searchValues = 'Cancer,Immunotherapy'# replace with the search words you want to search for
    csvList=util.getheadlineswithkeywords(searchValues)
    return csvList

# function uploads the headlines corresponding to each keyword in seperate folder
def s3upload(**kwargs):
	ti = kwargs['ti']
	v1 = ti.xcom_pull(task_ids='gettingheadlines')# using xcom_pull to get headlines csv files
	util.uploadCsvToS3(v1)

# defining the tasks
get_headlines_task = PythonOperator(
	task_id='gettingheadlines',
	python_callable=getHeadlines,
	provide_context=True,
	op_kwargs={'base_dir': base_dir},
	dag=dag)

aws_upload_task = PythonOperator(
	task_id='uploadingtos3',
	python_callable=s3upload,
	provide_context=True,
	op_kwargs={'base_dir': base_dir},
	dag=dag)

# define the dependencies between the tasks
aws_upload_task.set_upstream(get_headlines_task)
