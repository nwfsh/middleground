from datetime import datetime
from airflow import DAG # no worries abt yellow lines its installed inisde docker not in my .venv
from airflow.operators.python import PythonOperator
from ingestion.bronze_youtube_ingest import ingest_video


VIDEO_IDS = [
    "giZy9gEydzM",
    "VtuuvyLEmG4",
]

def pull_all_videos():
    for video_id in VIDEO_IDS:
        ingest_video(video_id)

with DAG(
    dag_id="youtube_bronze_ingest",
    start_date=datetime(2026, 9, 1),
    schedule_interval="@daily",
    catchup=False, # we already got ALL the comments through the API, so keep false 
                    # you will burn through credits telling them to keep repulling from 0 again
) as dag:
    pull_task = PythonOperator(
        task_id="pull_youtube_data",
        python_callable=pull_all_videos,
    )

