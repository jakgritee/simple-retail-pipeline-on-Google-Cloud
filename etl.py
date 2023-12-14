from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.bash import BashOperator

import pendulum
import pandas as pd
import requests

@dag(
    schedule_interval='@once',
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['verson4']
)

def etl():
    """ data pipeline in course Road to Data Engineer
    """
    @task()
    def extract_from_db():
        """ get data from MySQL database, 
            convert to Dataframe, 
            spare InvoiceTimestamp column for later use
            and write to csv
        """
        conn = MySqlHook('db4free-connection')

        records = conn.get_records('select * from online_retail;')

        retail = pd.DataFrame(records)
        retail['InvoiceTimestamp'] = pd.to_datetime(retail['InvoiceDate'])
        retail['InvoiceDate'] = pd.to_datetime(retail['InvoiceDate']).dt.date
        retail = retail.rename(columns={'UnitPrice': 'GBPPrice'})
        retail.to_csv('/home/airflow/gcs/data/retail_from_db.csv', index=False)
    
    @task()
    def extract_from_api():
        """ get data from api, 
            convert to Dataframe and reset index, 
            convert datatype to date
            and write to csv
        """
        r = requests.get('https://gbp-to-thb-4tbescyvrq-ts.a.run.app/')
        j = r.json()

        exchange_rate = pd.DataFrame(j).reset_index().rename(columns={'index': 'date'})
        exchange_rate['date'] = pd.to_datetime(exchange_rate['date']).dt.date
        exchange_rate.to_csv('/home/airflow/gcs/data/rate_from_api.csv', index=False)

    @task()
    def convert_to_thb():
        """ merge data, 
            create column THBPrice
            and write to csv
        """
        retail = pd.read_csv('/home/airflow/gcs/data/retail_from_db.csv')
        exchange_rate = pd.read_csv('/home/airflow/gcs/data/rate_from_api.csv')

        df = retail.merge(exchange_rate, how='left', left_on='InvoiceDate', right_on='date')
        df['THBPrice'] = df.apply(lambda x: x['GBPPrice'] * x['rate'], axis=1)
        df.to_csv('/home/airflow/gcs/data/cleaned_data.csv', index=False)

    create_dateset_in_bigquery = BashOperator(
    task_id='create_dateset_in_bigquery',
    bash_command='bq --location=australia-southeast1 mk \
                    --dataset sales',
    )

    load_into_bigquery = BashOperator(
    task_id='load_to_bigquery',
    bash_command='bq --location=australia-southeast1 load \
                    --source_format=CSV \
                    --autodetect=true sales.online_retail \
                    gs://[BUCKET_NAME]/data/cleaned_data.csv',
    )

    [extract_from_db(), extract_from_api()] >> convert_to_thb() >> create_dateset_in_bigquery >> load_into_bigquery

dag = etl()
