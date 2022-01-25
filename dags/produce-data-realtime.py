import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from produce_data_module import produce_data
from airflow.operators.python_operator import PythonOperator
from time import sleep
from kafka import KafkaProducer
import json



# DAG Definition
###############################################
now = datetime.now()
default_args = {
    "owner": "bigdatateam",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 21, 13, 50),
    "email": ["tranbinhluat@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}


dag = DAG(
    dag_id="produce_data",
    start_date= datetime(2022, 1, 21, 13, 50),
    schedule_interval=timedelta(days=1),
)


def producer_kafka():
    df = produce_data.produce_data()
    producer = KafkaProducer(bootstrap_servers=['kafka'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    topic = "web_data"
    std = produce_data.StdoutListener(producer,topic)    
    std.on_data(df)
    #return True


start = DummyOperator(task_id="start", dag=dag)


end = DummyOperator(task_id="end", dag=dag)

producer_kafka_task = PythonOperator(
      task_id='producer_kafka',
      python_callable=producer_kafka,
      dag=dag
    )

start >> producer_kafka_task >> end
