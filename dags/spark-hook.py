import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/local/spark/resources/jars/postgresql-9.4.1207.jar"

postgres_db = "jdbc:postgresql://postgres/airflow"
postgres_user = "airflow"
postgres_pwd = "airflow"

###############################################
# DAG Definition
###############################################

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
    dag_id="spark_hook",
    start_date= datetime(now.year, now.month, now.day),
    schedule_interval="0 2 * * MON-FRI",
)

start = DummyOperator(task_id="start", dag=dag)


spark_job_load_postgres = SparkSubmitOperator(
    task_id="spark_job_load_postgres",
    application="/usr/local/spark/app/spark-hook.py", # Spark application path created in airflow and spark cluster
    name="hook_data",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[postgres_db,postgres_user,postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_job_load_postgres >> end
