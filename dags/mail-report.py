import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.sensors.file_sensor import FileSensor


now = datetime.now()
default_args = {
    "owner": "bigdatateam",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["tranbinhluat@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="test_mail_report",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 2 * * MON-FRI",
)

start = DummyOperator(task_id="start", dag=dag)

email = EmailOperator(
        task_id='send_email_report',
        to='tranbinhluat@gmail.com',
        subject='Daily Report',
        html_content=""" <h3>Email Test</h3> """,
        dag=dag
)

precheck = FileSensor(
    task_id='check_for_datafile',
    filepath='/usr/local/report/19012022.xlsx',
    start_date=datetime(2022,1,21),
    mode='reschedule',
    dag=dag)



end = DummyOperator(task_id="end", dag=dag)


start >> precheck >> email >> end


