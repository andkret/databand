import logging
import pandas as pd
from sodapy import Socrata
from datetime import datetime

now = datetime.now()

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

def transform_data(im_extracted):
    
    print(im_extracted['agency_name'].value_counts())
    
    # Filter just after Department of finance data
    ex_extracted = im_extracted.loc[im_extracted['agency_name']=="Department of Finance"]
    print(ex_extracted.head(5))
    return ex_extracted

def load_data(im_transformed):
    print(im_transformed.shape)

    current_time = now.strftime("%Y-%M-%d %H:%M:%S")
    
    #save file in subfolder /data
    im_transformed.to_csv('./data/311calls_'+ current_time +'_.csv')

extracted = extract_from_api()
transformed = transform_data(extracted)
load_data(transformed)






