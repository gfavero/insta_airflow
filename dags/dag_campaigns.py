import requests
from datetime import datetime,timedelta
from airflow.models import DAG
from airflow.models import Variable,Connection
from airflow.decorators import dag, task
import json
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
from airflow.operators.python_operator import PythonOperator


# Variables **
access_token = Variable.get("access_token")
client_id = Variable.get("client_id")
client_secret = Variable.get("client_secret")
ig_username = Variable.get("ig_username")
endpoint_base = Variable.get("endpoint_base") 
account_id_pri = Variable.get("account_id_pri") 

PROJECT_ID="instagram-project-337102"
DATASET = "insta_database"

@task(task_id="task_geAddAccountIds")
def geAddAccountIds(**kwargs):
    credentials = service_account.Credentials.from_service_account_file( '/opt/airflow/plugins/key.json')
    client = bigquery.Client(credentials=credentials)
    query = f'''
                SELECT id FROM {PROJECT_ID}.{DATASET}.adAccounts
            '''
    df = client.query(query).to_dataframe()  
    for id in df.id:
        print(id)
    # return df.id.to_dict()
    return dict(zip(df.id, df.index))

def getMetaCampaigns(pagingUrl, account_id):
    endpointParams = dict()
    endpointParams['fields'] =  'name,id,campaigns { name,id, stop_time,start_time,status}'
    endpointParams['access_token'] = access_token# access token
    if (pagingUrl == '') : # get first page
      url = endpoint_base + account_id # endpoint url
      data = requests.get( url, endpointParams )
      response = dict()
      response['json_data'] = json.loads( data.content )
    else : # get next page
      url = pagingUrl  # endpoint url for next page
      data = requests.get( url)
      response = dict()
      response['json_data'] = json.loads( data.content )
    try:
      try:
        response['next'] = response['json_data']['campaigns']['paging']['next']
      except:
        response['next'] = response['json_data']['paging']['next']
    except:
      response['next'] = ''
    return response

@task(task_id="task_getMetaCampaigns")
def ETL_MetaCampaigns(**kwargs):
    ti = kwargs['ti']
    adaccounts_list = ti.xcom_pull(task_ids='task_geAddAccountIds')
    df_campaigns= pd.DataFrame(columns=['extracted_date','account_id','account_name','campaign_id','campaign_name','start_time','stop_time','status',])
    credentials = service_account.Credentials.from_service_account_file( '/opt/airflow/plugins/key.json')
    
    # for id in adaccounts_list.id:
    for id in adaccounts_list:
        global next
        global account_id
        global account_name
        # Get campaigns for each account
        # campaigns = getMetaCampaigns('',id)
        campaigns = getMetaCampaigns('',id)
        
        # Store next cursor, account_id, and account_name from the campaign response
        next = campaigns['next']
        account_id = campaigns['json_data']['id']    
        account_name = campaigns['json_data']['name']
        try:
            for i in range(len(campaigns['json_data']['campaigns']['data'])):
                # Store the campaign information in a dictionary
                campaigns_dict = {
                                    'campaign_id'   : campaigns['json_data']['campaigns']['data'][i].get('id',None),
                                    'campaign_name' : campaigns['json_data']['campaigns']['data'][i].get('name',None),
                                    'start_time'    : campaigns['json_data']['campaigns']['data'][i].get('start_time', None),
                                    'stop_time'     : campaigns['json_data']['campaigns']['data'][i].get('stop_time', None),
                                    'status'        : campaigns['json_data']['campaigns']['data'][i].get('status', None),
                                }  

                df_campaigns.loc[len(df_campaigns)] = [ (pd.to_datetime('today').now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S'),
                                                        account_id,
                                                        account_name,
                                                        campaigns_dict ['campaign_id'],
                                                        campaigns_dict ['campaign_name'],
                                                        campaigns_dict ['start_time'][:10] if campaigns_dict ['start_time'] else campaigns_dict ['start_time'],
                                                        campaigns_dict ['stop_time'][:10] if campaigns_dict ['stop_time'] else campaigns_dict ['stop_time'],
                                                        campaigns_dict ['status'],
                                                    ]
            # Continue to access the next set of campaigns if the next cursor is not an empty string
            while (next!=''):
                campaigns = getMetaCampaigns(next,account_id)    
                next = campaigns['next']
                for j in range(len(campaigns['json_data']['data'])):
                    # Store the campaign information in a dictionary
                    campaigns_dict =    {
                                            'campaign_id'   : campaigns['json_data']['data'][j].get('id',None),
                                            'campaign_name' : campaigns['json_data']['data'][j].get('name',None),
                                            'start_time'    : campaigns['json_data']['data'][j].get('start_time', None),
                                            'stop_time'     : campaigns['json_data']['data'][j].get('stop_time', None),
                                            'status'        : campaigns['json_data']['data'][j].get('status', None),
                                        } 
                    df_campaigns.loc[len(df_campaigns)] =  [ (pd.to_datetime('today').now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S'),
                                                            account_id,
                                                            account_name,
                                                            campaigns_dict ['campaign_id'],
                                                            campaigns_dict ['campaign_name'],
                                                            campaigns_dict ['start_time'][:10] if campaigns_dict ['start_time'] else campaigns_dict ['start_time'],
                                                            campaigns_dict ['stop_time'][:10] if campaigns_dict ['stop_time'] else campaigns_dict ['stop_time'],
                                                            campaigns_dict ['status'],
                                                        ]
            df_campaigns.to_gbq( destination_table=f'{DATASET}.Campaigns',  project_id=PROJECT_ID, credentials=credentials, if_exists="replace" )
        except Exception as e:
                print(campaigns['json_data'])
                print("Data extract error 127: " + str(e)) 
                                           

default_args = {
    'owner': 'Gefa',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date':  datetime(2023,2,26),
    'retry_delay': timedelta(minutes=5),
    'catchup' : False
}

with DAG('ETL_MetaCampaigns', schedule_interval=timedelta(days=1), default_args=default_args, tags=['bigquery_gcp', 'api_Meta'] ) as dag:

    
    task_01_geAddAccountIds = geAddAccountIds()
    task_02_ETL_MetaCampaigns = ETL_MetaCampaigns()

task_01_geAddAccountIds >> task_02_ETL_MetaCampaigns
