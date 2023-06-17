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

#google api
LOCATION = "US"
GOOGLE_CONN_ID = "google_cloud_default"


@task(task_id="task_getCampaigns")
def getCampaigns(**kwargs):
    credentials = service_account.Credentials.from_service_account_file('/opt/airflow/plugins/key.json')
    client = bigquery.Client(credentials=credentials)
    query = f'''
                SELECT campaign_id FROM {PROJECT_ID}.{DATASET}.Campaigns
                where status = 'ACTIVE'
            '''
    df = client.query(query).to_dataframe()  
    return dict(zip(df.campaign_id, df.index))

def getInsights(date,id):
    """
    "https://graph.facebook.com/<API_VERSION>/<CAMPAIGN_ID>/insights"
    Reference = https://developers.facebook.com/docs/marketing-api/reference/ads-insights/
    """
    url = endpoint_base + id +'/insights'  
    endpointParams = dict() # parameter to send to the endpoint
    endpointParams['fields'] = '''  campaign_id,
                                    ad_id,
                                    adset_id,
                                    impressions,
                                    spend,
                                    cost_per_unique_click,
                                    ad_name,
                                    adset_name,
                                    clicks,
                                    conversions,
                                    social_spend,
                                    cpc,
                                    action_values,
                                    actions'''
    endpointParams['time_range[since]'] = date
    endpointParams['time_range[until]'] = date
    endpointParams['access_token'] = access_token
    data = requests.get(url,endpointParams)
    response = dict()
    response['json_data'] = json.loads(data.content)
    return response

@task(task_id="task_getInsights")
def ETL_MetagetInsights(**kwargs):
    ti = kwargs['ti']
    update_date = ti.xcom_pull(task_ids='get_date')
    campaigns_list = ti.xcom_pull(task_ids='task_getCampaigns')
    credentials = service_account.Credentials.from_service_account_file('/opt/airflow/plugins/key.json')
    client = bigquery.Client(credentials=credentials)
    global count_loop
    count_loop = 0

    day= (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    print(f''' Reference date = {day}''')
  
    for camp_id in campaigns_list:
        count_loop += 1
        salesInsights = getInsights(day,camp_id)
        # Store the insights information in a dictionary
        insights_dict = {
                            'campaign_id': camp_id,
                            'impressions': None,
                            'spend': None,
                            'cost_per_unique_click': None,
                            'purchases_convertion_value': None,
                            'clicks': None,
                            'cpc': None,
                            'date_start': None,
                            'date_stop': None,
                            'view_content': None,
                            'purchase_qty': None,
                            'initiate_checkout': None,
                            'post_engagement': None,
                            'landing_page_view': None,
                            'post_reaction': None,
                            'link_click': None,
                            'comment': None,
                        }
        try:  
            len(salesInsights['json_data']['data'])
        except Exception as e:
            print(f'API Error Message: {salesInsights["json_data"]}') # get error message from the facebook API
           
        if len(salesInsights['json_data']['data'])>0:
            insights_dict['impressions']                  = salesInsights['json_data']['data'][0].get('impressions', None)
            insights_dict['spend']                        = salesInsights['json_data']['data'][0].get('spend', None)
            insights_dict['cost_per_unique_click']        = salesInsights['json_data']['data'][0].get('cost_per_unique_click', None)
            insights_dict['clicks']                       = salesInsights['json_data']['data'][0].get('clicks', None)
            insights_dict['cpc']                          = salesInsights['json_data']['data'][0].get('cpc', None)
            insights_dict['date_start']                   = salesInsights['json_data']['data'][0].get('date_start', None)
            insights_dict['date_stop']                    = salesInsights['json_data']['data'][0].get('date_stop', None)

            action_values_list = salesInsights['json_data']['data'][0].get('action_values', None)
            while action_values_list:
                action_values_dic = action_values_list.pop(-1)
                if action_values_dic.get('action_type', None) == 'omni_purchase':
                    insights_dict['purchases_convertion_value'] = action_values_dic.get('value', None)
            
            actions_list = salesInsights['json_data']['data'][0].get('actions', None)
            while actions_list:
                actions_list_dic = actions_list.pop(-1)
                if actions_list_dic.get('action_type', None) == 'omni_purchase':
                    insights_dict['purchase_qty'] =  actions_list_dic.get('value', None)
                elif actions_list_dic.get('action_type', None) == 'view_content':
                    insights_dict['view_content'] =  actions_list_dic.get('value', None)
                elif actions_list_dic.get('action_type', None) == 'initiate_checkout':
                    insights_dict['initiate_checkout'] =  actions_list_dic.get('value', None)
                elif actions_list_dic.get('action_type', None) == 'post_engagement':
                    insights_dict['post_engagement'] =  actions_list_dic.get('value', None)
                elif actions_list_dic.get('action_type', None) == 'landing_page_view':
                    insights_dict['landing_page_view'] =  actions_list_dic.get('value', None)
                elif actions_list_dic.get('action_type', None) == 'post_reaction':
                    insights_dict['post_reaction'] =  actions_list_dic.get('value', None)
                elif actions_list_dic.get('action_type', None) == 'link_click':
                    insights_dict['link_click'] =  actions_list_dic.get('value', None)
                elif actions_list_dic.get('action_type', None) == 'comment':
                    insights_dict['comment'] =  actions_list_dic.get('value', None)
        
            rows_to_insert =   [ {   
                                    u'campaign_id' : insights_dict['campaign_id'],
                                    u'impressions' : insights_dict['impressions'],
                                    u'spend' : insights_dict['spend'],
                                    u'cost_per_unique_click' : insights_dict['cost_per_unique_click'],
                                    u'purchases_convertion_value' : insights_dict['purchases_convertion_value'],
                                    u'clicks' : insights_dict['clicks'],
                                    u'cpc' : insights_dict['cpc'],
                                    u'date_start' : insights_dict['date_start'],
                                    u'date_stop' : insights_dict['date_stop'],
                                    u'view_content' : insights_dict['view_content'],
                                    u'purchase_qty' : insights_dict['purchase_qty'],
                                    u'initiate_checkout' : insights_dict['initiate_checkout'],
                                    u'post_engagement' : insights_dict['post_engagement'],
                                    u'landing_page_view' : insights_dict['landing_page_view'],
                                    u'post_reaction' : insights_dict['post_reaction'],
                                    u'link_click' : insights_dict['link_click'],
                                    u'comment' : insights_dict['comment'],
                                },] 
            client.insert_rows_json( f'{DATASET}.CampaignsInsights', rows_to_insert )    



default_args = {
    'owner': 'Gefa',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date':  datetime(2023,6,11),
    'retry_delay': timedelta(minutes=5),
    'catchup' : False
}

with DAG('ETL_Meta_Get_Insights_daily', schedule_interval=timedelta(days=1), default_args=default_args, tags=['bigquery_gcp', 'api_Meta','Campaigns'] ) as dag:

    start  = DummyOperator(
        task_id = 'start',
        dag = dag
        )

    check_dataset_CampaignsInsights = BigQueryCheckOperator(
        task_id = 'check_dataset_CampaignsInsights',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.CampaignsInsights`'
        )

    task_02_getCampaigns = getCampaigns()
    task_03_ETL_MetagetInsights= ETL_MetagetInsights()

    final_check_dataset_CampaignsInsights = BigQueryCheckOperator(
        task_id = 'final_check_dataset_CampaignsInsights',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.CampaignsInsights`'
        )

    end  = DummyOperator(
        task_id = 'end',
        dag = dag
        ) 

start >> check_dataset_CampaignsInsights >> task_02_getCampaigns >> task_03_ETL_MetagetInsights >> final_check_dataset_CampaignsInsights >> end
