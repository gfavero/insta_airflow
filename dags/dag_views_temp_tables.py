from datetime import timedelta, datetime
from airflow import DAG, settings
from airflow.utils.dates import days_ago
from airflow.models import Variable,Connection
from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from google.oauth2 import service_account

#graph facebook api
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


default_args = {
    'owner': 'Gefa',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date':  days_ago(0),
    'retry_delay': timedelta(minutes=5),
}

# Schedule to every hours starting in the minute 10
with DAG('dag_Views_Temp_Table', schedule_interval='10 * * * *', default_args=default_args , tags=['bigquery_gcp']) as dag:
    
    start  = DummyOperator(
        task_id = 'start',
        dag = dag
        )

    check_dataset_stories_temp = BigQueryCheckOperator(
        task_id = 'check_dataset_stories_temp',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.Stories_Temp`'
        )
    check_dataset_stories_insights_temp = BigQueryCheckOperator(
        task_id = 'check_dataset_stories_insights_temp',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.Stories_Insights_Temp`'
        )
    check_dataset_Business_temp = BigQueryCheckOperator(
        task_id = 'check_dataset_Business_temp',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT max(extracted_date), count(*) FROM `{PROJECT_ID}.{DATASET}.Business_Discovery_Temp`'
        )
    check_dataset_Business_daily = BigQueryCheckOperator(
        task_id = 'check_dataset_Business_daily',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT max(extracted_date), count(*) FROM `{PROJECT_ID}.{DATASET}.Business_Discovery_daily`'
        )
    final_check_dataset_stories_temp = BigQueryCheckOperator(
        task_id = 'final_check_dataset_stories_temp',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.Stories_Temp`'
        )
    final_check_dataset_stories_insights_temp = BigQueryCheckOperator(
        task_id = 'final_check_dataset_stories_insights_temp',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.Stories_Insights_Temp`'
        )
    final_check_dataset_Business_temp = BigQueryCheckOperator(
        task_id = 'final_check_dataset_Business_temp',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT max(extracted_date), count(*) FROM `{PROJECT_ID}.{DATASET}.Business_Discovery_Temp`'
        )
    final_check_dataset_Business_dayly = BigQueryCheckOperator(
        task_id = 'final_check_dataset_Business_dayly',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT max(extracted_date), count(*) FROM `{PROJECT_ID}.{DATASET}.Business_Discovery_daily`'
        )

    update_stories_temp_table = BigQueryOperator(
        task_id = 'update_stories_temp_table',
        use_legacy_sql=False,
        location = LOCATION,
        sql = './SQL/sql_stories_1_temp.sql'
        )

    remove_duplicated_stories = BigQueryOperator(
        task_id = 'remove_duplicated_stories',
        use_legacy_sql=False,
        location = LOCATION,
        sql = './SQL/sql_stories_2_remove_duplicated.sql'
        )
    
    update_stories_insights_temp_table = BigQueryOperator(
        task_id = 'update_stories_insights_temp_table',
        use_legacy_sql=False,
        location = LOCATION,
        sql = './SQL/sql_stories_insights_1_temp.sql'
        )
    remove_duplicated_stories_insights = BigQueryOperator(
        task_id = 'remove_duplicated_stories_insights',
        use_legacy_sql=False,
        location = LOCATION,
        sql = './SQL/sql_stories_insights_2_remove_duplicated.sql'
        )
    update_Business_temp = BigQueryOperator(
        task_id = 'update_Business_temp',
        use_legacy_sql=False,
        location = LOCATION,
        sql = './SQL/sql_Business_temp.sql'
        )
    update_Business_daily = BigQueryOperator(
        task_id = 'update_Business_daily',
        use_legacy_sql=False,
        location = LOCATION,
        sql = './SQL/sql_Business_daily.sql'
        )

    end  = DummyOperator(
        task_id = 'end',
        dag = dag
        ) 

start  >> [check_dataset_stories_temp , check_dataset_stories_insights_temp,check_dataset_Business_temp,check_dataset_Business_daily] 

check_dataset_stories_temp >> update_stories_temp_table >> remove_duplicated_stories >> final_check_dataset_stories_temp >> end

check_dataset_stories_insights_temp >> update_stories_insights_temp_table >> remove_duplicated_stories_insights >> final_check_dataset_stories_insights_temp >> end

check_dataset_Business_temp  >> update_Business_temp  >> final_check_dataset_Business_temp  >> end
check_dataset_Business_daily >> update_Business_daily >> final_check_dataset_Business_dayly >> end
