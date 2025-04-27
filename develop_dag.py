from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException
from google.cloud import storage, bigquery
from datetime import datetime, timedelta
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# set this to your alert mail
ALERT_EMAIL = 'ellapunagaprasad3@gmail.com'

# file prefix to check in GCS bucket
BUCKET_NAME = 'april-bucket-practice'
FILE_PREFIX = 'PMS_DATA_DAY2'  # prefix without CSV
GCS_PATH = f'gs://{BUCKET_NAME}/{FILE_PREFIX}.CSV'

default_args = {
    'owner' : 'airflow',
    'retries' : 0,
    'email_on_failure' : False,
    'email' : [ALERT_EMAIL],
    'retry_delay' : timedelta(minutes=5),
}

def check_gcs_file(**kwargs):
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs(prefix=FILE_PREFIX))

    if not blobs:
        raise AirflowSkipException(f"No file with prefix '{FILE_PREFIX}' found in bucket '{BUCKET_NAME}'")
    return True
    
def insert_into_final_table():
    client = bigquery.Client()

    query1 = """
            INSERT INTO `rare-tome-458105-n0.pms_final_dataset.pms_table` (
            PM_ID,Name,FromState,MPFrom,Tenure_start_date,Tenure_end_date
            )
            SELECT PM_ID,Name,FromState,MPFrom,Tenure_start_date,Tenure_end_date
            FROM `rare-tome-458105-n0.pms_stage_dataset.pms_table`;
            """
    query_job = client.query(query1)
    query_job.result()

def check_duplicate():
    client = bigquery.Client()
    query2 = """    
            SELECT *, COUNT(*) AS NO_OF_ROWS FROM `rare-tome-458105-n0.pms_final_dataset.pms_table` GROUP BY 1,2,3,4,5,6
            HAVING COUNT(*) > 1;
            """
    query_job = client.query(query2)
    results = list(query_job)

    if results:
        query3 = """
            BEGIN
            CREATE OR REPLACE TEMP TABLE pms_table_temp AS
            SELECT * FROM `rare-tome-458105-n0.pms_final_dataset.pms_table` WHERE 1=2;

            
            INSERT INTO pms_table_temp
            SELECT * FROM `rare-tome-458105-n0.pms_final_dataset.pms_table` GROUP BY 1,2,3,4,5,6 HAVING COUNT(*)>1;

            
            DELETE FROM `rare-tome-458105-n0.pms_final_dataset.pms_table` WHERE pm_id IN
            (SELECT pm_id FROM (SELECT *, COUNT(*) FROM `rare-tome-458105-n0.pms_final_dataset.pms_table` GROUP BY 1,2,3,4,5,6 
            HAVING COUNT(*)>1));

            
            INSERT INTO `rare-tome-458105-n0.pms_final_dataset.pms_table`
            SELECT * FROM pms_table_temp;
            END
            """
        query_job = client.query(query3)
        query_job.result()

    else:
        return True

    

with DAG(
    dag_id = 'daily_pms_table_pipeline_v1',
    default_args = default_args,
    description = 'Loads MPs data daily from GCS to BQ if file exists',
    schedule_interval = None, # Daily at 2am
    start_date = days_ago(1),
    catchup = False,
    tags = ['pms','email','bigquery','daily'],
) as dag:
    
    task_check_file = PythonOperator(
        task_id = 'check_if_file_exists',
        python_callable = check_gcs_file,
        provide_context = True
    )

    task_load_to_staging = BashOperator(
    task_id='load_csv_data_to_stage_table',
    bash_command=f"""
        bq load \
        --project_id=rare-tome-458105-n0 \
        --skip_leading_rows=1 \
        --source_format=CSV \
        rare-tome-458105-n0:pms_stage_dataset.pms_table \
        {GCS_PATH} \
        'PM_ID:INT64,Name:STRING,FromState:STRING,MPFrom:STRING,Tenure_start_date:DATE,Tenure_end_date:DATE'
    """
    )

    task_insert_into_final = PythonOperator(
        task_id = 'insert_into_final_table',
        python_callable = insert_into_final_table
    ) 

    task_check_duplicate = PythonOperator(
        task_id = 'checking_duplicates_in_final_table',
        python_callable = check_duplicate
    )
    
    task_email_on_failure = EmailOperator(
        task_id = 'email_notify_failure',
        to = ALERT_EMAIL,
        subject = '[ALERT] DAG failed: {{dag.dag_id}}',
        html_content = """
            <h3>DAG: {{dag.dag_id}}<h3>
            <p>Task: {{task_instance.task_id}} failed.</p>
            <p>Execution Date: {{ ts }}</p>
        """,
        trigger_rule = 'one_failed'  # trigger only if previous task fails

    )

task_check_file >> task_load_to_staging >> task_insert_into_final >> task_check_duplicate >> task_email_on_failure