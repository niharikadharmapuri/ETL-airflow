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
dag = DAG('dag1', default_args=default_args,
          schedule_interval='0 0 * * *', max_active_runs=1) # scheduled to run everyday at midnight

util=Utility(news_api_key='',s3_bucket='')
# get all sources in english language 
def sources(**kwargs):
	#sourcesCsvString=util.getSources('business','en','in')
	sourcesCsvString=util.getSources( language='en')
	return sourcesCsvString

# get top headlines for list of sources given
def headlines(**kwargs):
	ti = kwargs['ti']
	v1 = ti.xcom_pull(task_ids='gettingsources')# xcom pull used to get values from the sources task
	csvFilesList=util.getheadlines(v1)
	return csvFilesList

# function uploads the headlines corresponding to each source in seperate folder
def s3upload(**kwargs):
	ti = kwargs['ti']
	v1 = ti.xcom_pull(task_ids='gettingheadlines')
	util.uploadCsvToS3(v1)

# defining the tasks
get_sources_task = PythonOperator(
	task_id='gettingsources',
	python_callable=sources,
	provide_context=True,
	op_kwargs={'base_dir': base_dir },
	dag=dag)

get_headlines_task = PythonOperator(
	task_id='gettingheadlines',
	python_callable=headlines,
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
get_headlines_task.set_upstream(get_sources_task)
aws_upload_task.set_upstream(get_headlines_task)
