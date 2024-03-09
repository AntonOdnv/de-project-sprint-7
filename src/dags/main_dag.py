from datetime import datetime
import os
import findspark
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

findspark.init()
findspark.find()

default_args = {
    "owner": "antodnv",
    "start_date": datetime(2022, 1, 1),
}

# На актуальных данных я бы настроил ежедневное исполнение от текущей даты с Catchup=True и глубиной 30 дней.
# Дату в качестве параметра я бы передавал текущую из окружения Airflow через Jinja.
_application_args=[
        "2022-05-27",
        "7", # нужно минимум 28 и более для определения домашнего города, но в учебных 5% столько дней нет :)
        "/user/antodnv/data/geo/events/",
        "/user/antodnv/data/geo/",
    ]
    
dag_main = DAG(
    dag_id="dag_main",
    default_args=default_args,
    schedule_interval=None,
    catchup = False
)

users_mart = SparkSubmitOperator(
    task_id="users_mart",
    dag=dag_main,
    application="/lessons/scripts/users_mart.py",
    conn_id="yarn_spark",
    application_args=_application_args,
    conf={"spark.driver.masResultSize": "20g"},
    executor_cores = 2,
    executor_memory = "2g"
)

geo_mart = SparkSubmitOperator(
    task_id="geo_mart",
    dag=dag_main,
    application="/lessons/scripts/geo_mart.py",
    conn_id="yarn_spark",
    application_args=_application_args,
    conf={"spark.driver.masResultSize": "20g"},
    executor_cores = 2,
    executor_memory = "2g"
)

recommendations_mart = SparkSubmitOperator(
    task_id="recommendations_mart",
    dag=dag_main,
    application="/lessons/scripts/recommendations_mart.py",
    conn_id="yarn_spark",
    application_args=_application_args,
    conf={"spark.driver.masResultSize": "20g"},
    executor_cores = 2,
    executor_memory = "2g"
)

users_mart >> geo_mart >> recommendations_mart