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
# instagram_account_id = '17841409476497010' # Nanda
# instagram_account_id = '17841400582900863' # pri - pudim
instagram_account_id_list = ['17841409476497010','17841400582900863']

PROJECT_ID="instagram-project-337102"
DATASET = "insta_database"

def getMedia(params, pagingUrl, instagram_account_id):
    if ( '' == pagingUrl ) : # get first page
      url = endpoint_base + instagram_account_id + '/media' # endpoint url
    else : # get specific page
      url = pagingUrl  # endpoint url
    endpointParams = dict() # parameter to send to the endpoint
    endpointParams['fields'] = 'id,ig_id,caption,comments_count,like_count,media_type,media_product_type,media_url,permalink,thumbnail_url,timestamp,username' # fields to get back
    endpointParams['access_token'] = access_token # access token
    data = requests.get( url, endpointParams )
    response = dict()
    response['endpointParams'] = endpointParams
    response['url'] = url
    response['json_data'] = json.loads( data.content ) 
    try:
      response['next'] = response['json_data']['paging']['next']
    except:
      pass
    return response

def getInsights(media_id,media_type):
    if media_type == 'CAROUSEL_ALBUM':
      params_metric = 'engagement,impressions,reach,saved,carousel_album_engagement,carousel_album_impressions,carousel_album_reach,carousel_album_saved,carousel_album_video_views'
    elif media_type == 'IMAGE':
      params_metric = 'engagement,impressions,reach,saved' 
    elif media_type == 'VIDEO':
      params_metric = 'engagement,impressions,reach,saved,video_views' 
    elif media_type == 'STORY':
      params_metric = 'impressions,reach,exits,taps_forward,taps_back,replies'
    else:
      params_metric = 'engagement,impressions,reach,saved' 

    endpointParams = dict() # parameter to send to the endpoint
    endpointParams['metric'] = params_metric
    endpointParams['access_token'] = access_token # access token
    url_insights = endpoint_base + media_id + '/insights'

    data = requests.get( url_insights, endpointParams )
    response = dict()
    response['url_insights'] = url_insights
    response['endpointParams'] = endpointParams
    response['json_data'] = json.loads(data.content)
    return response

def getStories(pagingUrl,instagram_account_id):
    """
    GET graph.facebook.com
    ?{ig-user-id}/stories?access_token={access-token}
    """
    if ( '' == pagingUrl ) : # get first page
      url = endpoint_base + instagram_account_id + '/stories' # endpoint url
    else : # get specific page
      url = pagingUrl  # endpoint url
    
    endpointParams = dict() # parameter to send to the endpoint
    endpointParams['access_token'] = access_token # access tokenid
    endpointParams['fields'] = 'id,caption,comments_count,like_count,media_type,media_product_type,media_url,permalink,thumbnail_url,timestamp' # fields to get back
    data = requests.get( url,endpointParams )
    response = dict()
    response['json_data'] = json.loads( data.content )        
    response['url'] = url
    response['endpointParams'] = endpointParams
    try:
      response['next'] = response['json_data']['paging']['next']
    except:
      pass
    return response

@task(task_id="Task_01_Get_Stories")
def getStoriesAPI(**kwargs):   
    # name = 'Pudim'
    stories_list =  dict()
    for instagram_account_id in instagram_account_id_list:
        credentials = service_account.Credentials.from_service_account_file( '/opt/airflow/plugins/key.json')
        client = bigquery.Client(credentials=credentials)
        for index in range(100):
            if index == 0:
                stories = getStories('',instagram_account_id)                  
            else:
                stories = getStories(next,instagram_account_id)
            try:
                for post in stories['json_data']['data']:
                    stories_list[post['id']] = True
                    try:
                        thumbnail_url= post['thumbnail_url']
                    except:
                        thumbnail_url=''
                    try:
                        permalink = post['permalink']
                    except:
                        permalink=''
                    try:
                        media_url = post['media_url']
                    except:
                        media_url=''
                    try:
                        caption = post['caption']
                    except:
                        caption=''
                    rows_to_insert = [{
                                        u'extracted_date': (pd.to_datetime('today').now() - timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S'),
                                        u'story_id': post['id'],
                                        u'caption': caption,
                                        u'comments_count': post['comments_count'],
                                        u'like_count': post['like_count'],
                                        u'media_type': post['media_type'],
                                        u'media_product_type': post['media_product_type'],
                                        u'media_url': media_url,
                                        u'permalink': permalink,
                                        u'thumbnail_url': thumbnail_url,                                
                                        u'created_date': (pd.to_datetime(post['timestamp'])- timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S'),
                                        u'name': instagram_account_id,

                                    },]
                    errors = client.insert_rows_json( f'{DATASET}.Stories' , rows_to_insert)
                    if errors == []:
                        pass
                        # print('New STORY has been added to google bigquery. ID= {} created_date {} '.format(post['id'],created_date))
                    else:
                        print(f'Errors while inserting rows: {errors}')
                if 'next' in stories:
                    next = stories['next']
                    print('Calling Next Story page API! line 148')
                else:
                    print('Break here there is no NEXT page! line 150')
                    break
            except Exception as e:
                errorAPI = stories['json_data']
                print(f' error line 154 {e} error API {errorAPI}')
                
                
    return  stories_list 

@task(task_id="Task_02_Get_Stories_insights")
def getStoriesInsightsAPI(**kwargs):   
    credentials = service_account.Credentials.from_service_account_file( '/opt/airflow/plugins/key.json')
    client = bigquery.Client(credentials=credentials)
    ti = kwargs['ti']
    stories_list = ti.xcom_pull(task_ids='Task_01_Get_Stories')
    for story in stories_list:
        insight = getInsights(story,'STORY')
        try:
            impressions_values = insight['json_data']['data'][0]['values'][0]['value'] #impressions_values
        except:
            impressions_values= None
        try:
            reach_values = insight['json_data']['data'][1]['values'][0]['value'] #reach_values
        except:
            reach_values= None
        try:
            exits_values = insight['json_data']['data'][2]['values'][0]['value'] #exits_values
        except:
            exits_values= None
        try:
            taps_forward = insight['json_data']['data'][3]['values'][0]['value'] #taps_forward
        except:
            taps_forward= None
        try:
            taps_back = insight['json_data']['data'][4]['values'][0]['value'] #taps_back
        except:
            taps_back= None
        try:
            replies_value = insight['json_data']['data'][5]['values'][0]['value'] #replies_value
        except:
            replies_value= None
        
        rows_to_insert = [{
                            u'extracted_date' : (pd.to_datetime('today').now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S'),
                            u'story_id': story,
                            u'impressions': impressions_values, 
                            u'reach': reach_values,
                            u'exits': exits_values, 
                            u'taps_forward': taps_forward, 
                            u'taps_back': taps_back, 
                            u'replies': replies_value,                
                        },]
        errors = client.insert_rows_json( f'{DATASET}.Stories_Insights' , rows_to_insert)
        if errors == []:
            pass
        else:
            print(f'Errors while inserting rows: {errors}')
     

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

with DAG('ETL_Get_Stories', schedule_interval='0 * * * *', default_args=default_args,tags=[ 'instagram','bigquery_gcp', 'api_Meta'] ) as dag:

    task_01_Get_Stories = getStoriesAPI()
    task_02_Get_Stories_insights = getStoriesInsightsAPI()

task_01_Get_Stories >> task_02_Get_Stories_insights
