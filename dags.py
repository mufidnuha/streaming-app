from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

app = [
    {
        "app_name":"wetv",
        "gp_id":"com.tencent.qqlivei18n",
        "apple_id":"1441531611"
    },
    {
        "app_name":"netflix",
        "gp_id":"com.netflix.mediaclient",
        "apple_id":"363590051"
    },
    {
        "app_name":"viu",
        "gp_id":"com.vuclip.viu",
        "apple_id":"1044543328"
    },
    {
        "app_name":"vidio",
        "gp_id":"com.vidio.android",
        "apple_id":"1048858798"
    },
    {
        "app_name":"disney_hotstar",
        "gp_id":"in.startv.hotstar.dplus",
        "apple_id":"1524156685"
    },
]
language='id'
country='id'

default_args = {
    'owner': 'mufida',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 2),
    'email': ['mufidanuha@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='streaming_app_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

el_gp_wetv = BashOperator(
    task_id="wetv from google play",
    bash_command="python3 ./ingest_google_play.py",
    params={'app_id': app[0]['gp_id'], 
                'app_name':app[0]['app_name'],
                'language':language,
                'country':country},
    dag=dag)

el_gp_netflix = BashOperator(
    task_id="netflix from google play",
    bash_command="python3 ./ingest_google_play.py",
    params={'app_id': app[1]['gp_id'], 
                'app_name':app[1]['app_name'],
                'language':language,
                'country':country},
    dag=dag)

el_gp_viu = BashOperator(
    task_id="viu from google play",
    bash_command="python3 ./ingest_google_play.py",
    params={'app_id': app[2]['gp_id'], 
                'app_name':app[2]['app_name'],
                'language':language,
                'country':country},
    dag=dag)

el_gp_vidio = BashOperator(
    task_id="vidio from google play",
    bash_command="python3 ./ingest_google_play.py",
    params={'app_id': app[3]['gp_id'], 
                'app_name':app[3]['app_name'],
                'language':language,
                'country':country},
    dag=dag)

el_gp_disney = BashOperator(
    task_id="disney hotstar from google play",
    bash_command="python3 ./ingest_google_play.py",
    params={'app_id': app[4]['gp_id'], 
                'app_name':app[4]['app_name'],
                'language':language,
                'country':country},
    dag=dag)


