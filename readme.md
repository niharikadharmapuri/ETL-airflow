# news-api Airflow pipeline:
This project extracts data from news.api.org and uploads the transformed data into AWS S3 bucket using Airflow.

## The project has three files:
1. dag1.py
2. dag2.py
3. utils.py

`tempus_dag.py` schedules three tasks 
1. Task One: Fetches all sources for English language.
2. Task Two: Fetches top-headlines for all the English language sources. Formats and creates .csv files for each source and returns list of csv filenames.
3. Task Three: Uploads the list of csvfiles to s3 and deletes the files once uploaded.

`dag2.py` schedules two tasks
1. Task One: Fetches top-headlines based on given keywords string. Formats and creates .csv files for each keyword and returns list of csv filenames.
2. Task Two: Uploads the list of csvfiles to s3 and deletes the files once uploaded.

`utils.py` has utility functions to fetch data from newsapi and upload csv files to aws-s3 these functions are shared between above mentioned dags.

    config = {
        'news_api_key': '',
        's3_bucket': ''
    }

Above config in `utils.py` should be updated with your `newsapi key` and `s3_bucket name` to which the csv files have to be uploaded. 

## Installation:
You need to install airflow using<br/>
		`pip install airflow`

You need to install dependent packages from requirements.txt using<br/>
	`pip install -r requirements.txt`

## Running the airflow:
copy `*.py` files into dags folder in  /Users/{username}/airflow<br/>
if dags folder doesn't exist create a dags folder and copy.

Once copied open terminal run<br/>
	    `airflow webserver`

In a new tab run<br/>
		`airflow scheduler`

open airflow UI<br/>
	`http://localhost:8080/admin/`

Turn on the toggle for dags named `Tempus_dag` and `Tempus_bonus_dag` they should be scheduled to run daily once. 

However, you could manually trigger the dag run by clicking the play button in the links column.
