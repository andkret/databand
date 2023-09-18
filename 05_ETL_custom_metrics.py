import logging
import pandas as pd
from sodapy import Socrata
from datetime import datetime
from dbnd import dbnd_tracking , task , dataset_op_logger, log_metric #add log_metric here to track custom metrics
import random

now = datetime.now()

@task
def extract_from_api():
    logging.basicConfig(level=logging.INFO)

    #Configure the dataset logger as a read dataset
    with dataset_op_logger(
        "dslogger/APIData",
        "read",
        with_preview=True,
    ) as logger:
        client = Socrata("data.cityofnewyork.us", None)

        # Unauthenticated client only works with public data sets. Note 'None'
        # in place of application token, and no username or password:
        results = client.get("wewp-mm3p", limit=100)

        # Convert to pandas DataFrame
        results_df = pd.DataFrame.from_records(results)
        print(results_df.dtypes)
        print(results_df.shape)
        print(results_df.head(5))
        
        logger.set(data=results_df) # send the dataset to the logger

    return results_df

@task
def transform_data(im_extracted):
    
    print(im_extracted['agency_name'].value_counts())

    # Filter just after Department of finance data    
    ex_extracted = im_extracted.loc[im_extracted['agency_name']=="Department of Finance"]
    print(ex_extracted.head(5))

    # Configure our random metric and log it to databand
    num1 = random.randint(10, 20)
    
    log_metric("My_Random_Metric", num1)
    
    return ex_extracted

@task
def load_data(im_transformed):
    
    #Configure the dataset logger for this dataset as a write dataset
    with dataset_op_logger(
        "dslogger/DeptOfFinanceData",
        "write",
        with_preview=True,
    ) as logger:
    
        print(im_transformed.shape)

        current_time = now.strftime("%Y-%M-%d %H:%M:%S")
        
        #save file in subfolder /data
        im_transformed.to_csv('./data/311calls_'+ current_time +'_.csv')

        logger.set(data=im_transformed) # send the dataset to the logger

#configure databand connection
with dbnd_tracking(

    conf={
        "core": {
            "databand_url": "https://lde.databand.ai",
            "databand_access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJmcmVzaCI6ZmFsc2UsImlhdCI6MTY5NDA3OTgxMCwianRpIjoiMDM4ZmE3YWQtYWQwZi00YzE1LWJkZDItZWU1NmRlNGU0MGU3IiwidHlwZSI6ImFjY2VzcyIsImlkZW50aXR5IjoiYW5kcmVhc0BsZWFybmRhdGFlbmdpbmVlcmluZy5jb20iLCJuYmYiOjE2OTQwNzk4MTAsImV4cCI6MTc1NzE1MTgxMCwidXNlcl9jbGFpbXMiOnsiZW52IjoibGRlIn19.QXdLKjW4y_mGnyNLO5ro4bNJeJzmfYQdFyLTjAV7ZwU",
        },
    },
    
    job_name="ETL_custom_metrics",
    run_name='run'+ now.strftime("%Y-%M-%d %H:%M:%S"),
    project_name='Learn_Data_Engineering'
):

    #run the ETL pipeline
    extracted = extract_from_api()
    transformed = transform_data(extracted)
    load_data(transformed)











