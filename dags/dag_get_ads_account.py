import requests
from datetime import datetime,timedelta
from airflow.models import DAG
from airflow.models import Variable,Connection
from airflow.decorators import dag, task
import json
import pandas as pd
from google.oauth2 import service_account


# Variables
access_token    = Variable.get("access_token")
client_id       = Variable.get("client_id")
client_secret   = Variable.get("client_secret")
ig_username     = Variable.get("ig_username")
endpoint_base   = Variable.get("endpoint_base") 
account_id_pri  = Variable.get("account_id_pri") 

PROJECT_ID="instagram-project-337102"
DATASET = "insta_database"

def getAdAccounts():
    url = endpoint_base + ig_username # endpoint url
    endpointParams = dict() # parameter to send to the endpoint
    endpointParams['fields'] = 'adaccounts{name,balance,currency}' # fields to get back
    endpointParams['access_token'] = access_token # access token
    data = requests.get( url, endpointParams )
    response= json.loads( data.content ) 
    # print(json.dumps(response,indent=4))
    return response

@task(task_id="task_ETL_MetaAdAccounts")
def getMetaAdAccounts():
    try:
        credentials = service_account.Credentials.from_service_account_file( '/opt/airflow/plugins/key.json')
        AdAccounts = getAdAccounts()
        df_adaccounts= pd.DataFrame(columns=['extracted_date','name','id','balance','currency'])
        for i in range(len(AdAccounts['adaccounts']['data'])):
            AdAccounts_dict =   {
                                'name'      : AdAccounts['adaccounts']['data'][i].get('name',None),
                                'id'        : AdAccounts['adaccounts']['data'][i].get('id',None),
                                'balance'   : AdAccounts['adaccounts']['data'][i].get('balance', None),
                                'currency'  : AdAccounts['adaccounts']['data'][i].get('currency', None),
                                }  

            df_adaccounts.loc[len(df_adaccounts)] = [
                                                    (pd.to_datetime('today').now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S'),
                                                    AdAccounts_dict ['name'],
                                                    AdAccounts_dict ['id'],
                                                    AdAccounts_dict ['balance'],
                                                    AdAccounts_dict ['currency'],
                                                    ]
            df_adaccounts.to_gbq( destination_table=f'{DATASET}.adAccounts',  project_id=PROJECT_ID, credentials=credentials, if_exists="replace" )
    except Exception as e:
            print("Data extract error 1: " + str(e))                                             

default_args = {
    'owner': 'Gefa',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date':  datetime(2023,2,23),
    'retry_delay': timedelta(minutes=5),
    'catchup' : False
}

with DAG('ETL_MetaAdAccount', schedule_interval=timedelta(days=1), default_args=default_args, tags=['bigquery_gcp', 'api_Meta'] ) as dag:

    task_01_MetaAdAccounts = getMetaAdAccounts()

task_01_MetaAdAccounts
