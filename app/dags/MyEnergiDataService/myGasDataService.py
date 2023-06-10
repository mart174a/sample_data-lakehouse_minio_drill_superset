from typing import List
import os                                   # contact to Operative System

import airflow
from airflow.decorators import dag, task

from decouple import config                 # secret configs 
import requests                             #  for making HTTP requests
import time
from datetime import datetime               # for date and time manipulation
from datetime import timedelta        # time, time time

from operator import itemgetter             # for sorting dicts
#from geopy.distance import distance         # calculate distance between geopos

import csv                                  # writing csv files

import json
import pendulum


default_task_args = {
    'retries' : 10,
    'retry_delay' : timedelta(minutes=1),
    'retry_exponential_backoff' : True,
}

def getData(url: str, dataDir: str, dataName: str, dataTimedate: datetime, params: dict) -> List[str]:
    """Gets data from a api"""
    
    fileNames = []
    index = 1
    offset = params['offset'] if 'offset' in params.keys() else 0
    limit = params['limit'] if 'limit' in params.keys() else 100
   
    print(f"Started fetching data..")
    
    while True:
        params['offset'] = offset
    
        response = requests.get(url, params=params)
        if response.status_code != requests.codes.ok:
            raise Exception('http not 200 ok', response.text)

        time_stamp = dataTimedate.isoformat(timespec='seconds').replace(':', '.')
        fileName = f'{dataDir}/{dataName}_{time_stamp}_#{index}.json'
        with open(fileName, "w+") as f:
            f.write(response.text)
        result = response.json()
        stored = len(result['records'])

        print(f"Stored : {stored} files. {offset + stored} total")

        fileNames.append(fileName)
        
        if stored <= limit:
            print(f"\nSuccessfully stored {offset+stored} files.")
            break
            
        offset += limit
        index += 1
    
    return fileNames

@task
def write_to_bucket(dataJsons, bucket_path):
    import pandas as pd
    from minio import Minio
    from io import BytesIO
    import os
    import json

    MINIO_BUCKET_NAME = 'prodex-data'

    MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
    MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')

    client = Minio("minio:9000", access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

    for prodex_json_filepath in dataJsons:

        with open(prodex_json_filepath, 'r') as jf:
            prodex_json = json.load(jf)

        df = pd.DataFrame(prodex_json['records'])
        print(df)
        file_data = df.to_parquet(index=False)

        prodex_filename = prodex_json_filepath.split('/')[-1]
        
        # Put parquet data in the bucket
        filename = (
            f"{bucket_path}/{prodex_filename}.parquet"
        )
        client.put_object(
            MINIO_BUCKET_NAME, filename, data=BytesIO(file_data), length=len(file_data), content_type="application/csv"
        )
        os.remove(prodex_json_filepath)


@task
def getNewestGasData(**kwargs):

    url = "https://api.energidataservice.dk/dataset/Gasflow"
    dataDir = "./dags/MyEnergiDataService/data"
    dataName = "GasFlow"
    timeStamp = datetime.fromisoformat(kwargs['ts'])
    params = {
        'limit': 1
    }
    return getData(url,dataDir,dataName,timeStamp,params)


@dag( 
    dag_id='Daily_Commercial_gas_amounts',
    schedule=timedelta(days=1),
    start_date=pendulum.datetime(2023, 6, 1, 15, 0, 0, tz="Europe/Copenhagen"),
    catchup=True,
    max_active_tasks=5,
    max_active_runs=5,
    tags=['experimental', 'gas', 'rest api'],
    default_args=default_task_args,)
def daily_commercial_gas_amounts():
    """
    Is set to run daily at 15 to fetch the newest data.
    it should be the data from the past day, because they update
    the data at 11.30 from the previous day.    
    """

    print("Doing DaylyGasAmounts")
    dataJson = getNewestGasData()
    write_to_bucket(dataJson, 'LatestGas')

daily_commercial_gas_amounts()