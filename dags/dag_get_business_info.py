import requests
from datetime import datetime,timedelta
from airflow.models import DAG
from airflow.models import Variable,Connection
from airflow.decorators import dag, task
import json
import pandas as pd
import time
from google.oauth2 import service_account
from google.cloud import bigquery


# Variables

access_token = Variable.get("access_token")
client_id = Variable.get("client_id")
client_secret = Variable.get("client_secret")
ig_username = Variable.get("ig_username")
endpoint_base = Variable.get("endpoint_base") 
account_id_pri = Variable.get("account_id_pri") 
instagram_account_id = '17841409476497010'
business_name = ['sopudimoficial', 'priscilamarcal_', 'nandadimaria','leandroladeiran']
# business_name = Variable.get("business_name")

PROJECT_ID="instagram-project-337102"
DATASET = "insta_database"

@task(task_id="Task_01_Get_business_info")
def getAccountInfo(): 
    def getAccountInfoAPI(userAccount): 
        """ Get info on a users account	
        API Endpoint:
        https://graph.facebook.com/{graph-api-version}/{ig-user-id}?fields=business_discovery.username({ig-username}){username,website,name,ig_id,id,profile_picture_url,biography,follows_count,followers_count,media_count}&access_token={access-token}
        """
        endpointParams = dict() # parameter to send to the endpoint
        endpointParams['fields'] = 'business_discovery.username(' + userAccount + '){username,website,name,ig_id,id,profile_picture_url,biography,followers_count,media_count}' # string of fields to get back with the request for the account
        endpointParams['access_token'] = access_token # access token
        url = endpoint_base + instagram_account_id # endpoint url
        data = requests.get( url,endpointParams )
        response = dict()
        response['json_data'] = json.loads(data.content )  
        return response

    credentials = service_account.Credentials.from_service_account_file( '/opt/airflow/plugins/key.json')
    client = bigquery.Client(credentials=credentials)
    for instagram_name in business_name:
        try:
            data = getAccountInfoAPI(instagram_name) 
            try:
                website = data['json_data']['business_discovery']['website']
            except:
                website = None

            rows_to_insert= [{
                                u'extracted_date' : (pd.to_datetime('today').now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S'),
                                u'username': data['json_data']['business_discovery']['username'],
                                u'website':  website,
                                u'name': data['json_data']['business_discovery']['name'], 
                                u'ig_id': data['json_data']['business_discovery']['ig_id'],
                                u'id': data['json_data']['business_discovery']['id'],
                                u'profile_picture_url': data['json_data']['business_discovery']['profile_picture_url'],
                                u'biography': data['json_data']['business_discovery']['biography'],
                                u'followers_count': data['json_data']['business_discovery']['followers_count'],
                                u'media_count': data['json_data']['business_discovery']['media_count']              
                                },]

            errors = client.insert_rows_json( f'{DATASET}.Business_Discovery' , rows_to_insert)
            time.sleep(1) 
            if errors == []:
                print('New Buniness has been added to google bigquery. name {} and followers = {}  '.format(data['json_data']['business_discovery']['username'],data['json_data']['business_discovery']['followers_count']))
            else:
                print(f'Errors while inserting rows: {errors}')
        except Exception as e:
            print(f'Errors while inserting rows error {e} ')

                

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

with DAG('ETL_Business_Info', schedule_interval='5 5 * * *', default_args=default_args,tags=[ 'instagram','bigquery_gcp', 'api_Meta'] ) as dag:

    task_01_getAccountInfo = getAccountInfo()

task_01_getAccountInfo
