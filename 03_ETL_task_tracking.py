import logging
import pandas as pd
from sodapy import Socrata
from datetime import datetime
from dbnd import dbnd_tracking , task #add tasks here to track tasks

now = datetime.now()
token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJmcmVzaCI6ZmFsc2UsImlhdCI6MTY5NDA3OTgxMCwianRpIjoiMDM4ZmE3YWQtYWQwZi00YzE1LWJkZDItZWU1NmRlNGU0MGU3IiwidHlwZSI6ImFjY2VzcyIsImlkZW50aXR5IjoiYW5kcmVhc0BsZWFybmRhdGFlbmdpbmVlcmluZy5jb20iLCJuYmYiOjE2OTQwNzk4MTAsImV4cCI6MTc1NzE1MTgxMCwidXNlcl9jbGFpbXMiOnsiZW52IjoibGRlIn19.QXdLKjW4y_mGnyNLO5ro4bNJeJzmfYQdFyLTjAV7ZwU"

@task
def extract_from_api():
    logging.basicConfig(level=logging.INFO)

    client = Socrata("data.cityofnewyork.us", None)

    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    results = client.get("wewp-mm3p", limit=100)

    # Convert to pandas DataFrame
    results_df = pd.DataFrame.from_records(results)
    print(results_df.dtypes)
    print(results_df.shape)
    print(results_df.head(5))

    return results_df

@task
def transform_data(im_extracted):
    
    print(im_extracted['agency_name'].value_counts())
    ex_extracted = im_extracted.loc[im_extracted['agency_name']=="Department of Finance"]
    print(ex_extracted.head(5))
    return ex_extracted

@task
def load_data(im_transformed):
    print(im_transformed.shape)

    
    current_time = now.strftime("%Y-%M-%d %H:%M:%S")
    
    im_transformed.to_csv('./data/311calls_'+ current_time +'_.csv')


with dbnd_tracking(

    conf={
        "core": {
            "databand_url": "https://lde.databand.ai",
            "databand_access_token": token,
        },
    },
    
    job_name="ETL_tasks_tracking",
    run_name='run'+ now.strftime("%Y-%M-%d %H:%M:%S"),
    project_name='Learn_Data_Engineering'
):

    extracted = extract_from_api()
    transformed = transform_data(extracted)
    load_data(transformed)











