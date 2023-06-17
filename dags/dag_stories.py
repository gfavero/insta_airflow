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
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator


# Variables

access_token = Variable.get("access_token")
client_id = Variable.get("client_id")
client_secret = Variable.get("client_secret")
ig_username = Variable.get("ig_username")
endpoint_base = Variable.get("endpoint_base") 
account_id_pri = Variable.get("account_id_pri") 
PROJECT_ID = Variable.get("project_id") 
DATASET = Variable.get("big_query_database") 

instagram_account_id_list = [] # accounts list
instagram_account_id_list.append(Variable.get('instagram_account_id'))

#google api
LOCATION = "US"
GOOGLE_CONN_ID = "google_cloud_default"

def getInsights(media_id,media_type):
    media_type_dic = {
                'CAROUSEL_ALBUM': 'engagement,impressions,reach,saved,carousel_album_engagement,carousel_album_impressions,carousel_album_reach,carousel_album_saved,carousel_album_video_views',
                'IMAGE'         : 'engagement,impressions,reach,saved',
                'VIDEO'         : 'engagement,impressions,reach,saved,video_views',
                'STORY'         : 'impressions,reach,exits,taps_forward,taps_back,replies',
                'OTHER'         : 'engagement,impressions,reach,saved',
                } 

    endpointParams = dict() # parameter to send to the endpoint
    endpointParams['metric'] = media_type_dic[media_type]
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
    if (pagingUrl == '' ) : # get first page
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
      response['next'] = ''
    return response

@task(task_id="Task_01_Get_Stories")
def getStoriesAPI(**kwargs):   
    stories_list =  dict() # Dictionary to hold stories IDs
    for instagram_account_id in instagram_account_id_list:
        print(instagram_account_id_list)
        credentials = service_account.Credentials.from_service_account_file( '/opt/airflow/plugins/key.json')
        client = bigquery.Client(credentials=credentials)
        global next
        stories = getStories('',instagram_account_id) 
        next = stories['next']      
      
        for i, story in enumerate(stories['json_data']['data']):
            id                  = story.get('id',None)
            caption             = story.get('caption',None)
            comments_count      = story.get('comments_count',None)
            like_count          = story.get('like_count',None)
            media_type          = story.get('media_type',None)
            media_product_type  = story.get('media_product_type',None)
            media_url           = story.get('media_url',None)
            permalink           = story.get('permalink',None)
            thumbnail_url       = story.get('thumbnail_url',None)
            timestamp           = story.get('timestamp',None)
            stories_list[id] = True

            rows_to_insert =   [{
                                u'extracted_date': (pd.to_datetime('today').now() - timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S'),
                                u'story_id': id,
                                u'caption': caption,
                                u'comments_count': comments_count,
                                u'like_count': like_count,
                                u'media_type': media_type,
                                u'media_product_type': media_product_type,
                                u'media_url': media_url,
                                u'permalink': permalink,
                                u'thumbnail_url': thumbnail_url,                                
                                u'created_date': (pd.to_datetime(timestamp)- timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S'),
                                u'name': instagram_account_id,
                                },]
            client.insert_rows_json( f'{DATASET}.Stories' , rows_to_insert) 
            # print(rows_to_insert)

            while (next!=''):
                stories = getStories(next,instagram_account_id)
                next = stories['next']  

                for j, jstory in enumerate(stories['json_data']['data']):
                    id                  = jstory.get('id',None)
                    caption             = jstory.get('caption',None)
                    comments_count      = jstory.get('comments_count',None)
                    like_count          = jstory.get('like_count',None)
                    media_type          = jstory.get('media_type',None)
                    media_product_type  = jstory.get('media_product_type',None)
                    media_url           = jstory.get('media_url',None)
                    permalink           = jstory.get('permalink',None)
                    thumbnail_url       = jstory.get('thumbnail_url',None)
                    timestamp           = jstory.get('timestamp',None)
                    stories_list[id] = True

                    rows_to_insert =   [{
                                        u'extracted_date': (pd.to_datetime('today').now() - timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S'),
                                        u'story_id': id,
                                        u'caption': caption,
                                        u'comments_count': comments_count,
                                        u'like_count': like_count,
                                        u'media_type': media_type,
                                        u'media_product_type': media_product_type,
                                        u'media_url': media_url,
                                        u'permalink': permalink,
                                        u'thumbnail_url': thumbnail_url,                                
                                        u'created_date': (pd.to_datetime(timestamp)- timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S'),
                                        u'name': instagram_account_id,
                                        },]
                    client.insert_rows_json( f'{DATASET}.Stories' , rows_to_insert) 
                    # print(rows_to_insert)
    return stories_list

@task(task_id="Task_02_Get_Stories_insights")
def getStoriesInsightsAPI(**kwargs):   
    credentials = service_account.Credentials.from_service_account_file( '/opt/airflow/plugins/key.json')
    client = bigquery.Client(credentials=credentials)
    ti = kwargs['ti']
    stories_list = ti.xcom_pull(task_ids='Task_01_Get_Stories')
    for story in stories_list:
        insight = getInsights(story,'STORY')

        if insight['json_data'].get('data', None):
            for i,insight_index in enumerate(insight['json_data']['data']):
                if insight_index.get('name', None) == 'impressions':
                    impressions_values =  insight_index.get('values', None)[0].get('value', None)
                elif insight_index.get('name', None) == 'reach':
                    reach_values =  insight_index.get('values', None)[0].get('value', None)
                elif insight_index.get('name', None) == 'exits':
                    exits_values =  insight_index.get('values', None)[0].get('value', None)
                elif insight_index.get('name', None) == 'taps_forward':
                    taps_forward =  insight_index.get('values', None)[0].get('value', None)
                elif insight_index.get('name', None) == 'taps_back':
                    taps_back =  insight_index.get('values', None)[0].get('value', None)
                elif insight_index.get('name', None) == 'replies':
                    replies_value =  insight_index.get('values', None)[0].get('value', None)
                
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
            client.insert_rows_json( f'{DATASET}.Stories_Insights' , rows_to_insert)
     

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

    start  = DummyOperator(
        task_id = 'start',
        dag = dag
        )

    check_dataset_Stories = BigQueryCheckOperator(
        task_id = 'check_dataset_Stories',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.Stories`'
        )
    check_dataset_Stories_Insights = BigQueryCheckOperator(
        task_id = 'check_dataset_Stories_Insights',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.Stories_Insights`'
        )

    task_01_Get_Stories = getStoriesAPI()
    task_02_Get_Stories_insights = getStoriesInsightsAPI()

    final_check_dataset_Stories = BigQueryCheckOperator(
        task_id = 'final_check_dataset_Stories',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.Stories`'
        )
    final_check_dataset_Stories_Insights = BigQueryCheckOperator(
        task_id = 'final_check_dataset_Stories_Insights',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.Stories_Insights`'
        )

    end  = DummyOperator(
        task_id = 'end',
        dag = dag
        ) 
start  >> [check_dataset_Stories , check_dataset_Stories_Insights] >> task_01_Get_Stories >> task_02_Get_Stories_insights
task_02_Get_Stories_insights >> [final_check_dataset_Stories,final_check_dataset_Stories_Insights] >> end

