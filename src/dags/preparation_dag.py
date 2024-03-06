from datetime import datetime
import os
import findspark
from airflow import DAG
from airflow.operators.bash import BashOperator

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

findspark.init()
findspark.find()

default_args = {
    "owner": "antodnv",
    "start_date": datetime(2022, 1, 1),
}
    
dag_preparation = DAG(
    dag_id="preparation_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup = False
)

# Загружаем актуальную версию по ссылке
get_geo_csv = BashOperator(
    task_id="get_geo_csv",
    dag=dag_preparation,
    bash_command = "wget https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv -O /lessons/geo.csv"
)

# Записываем полученный файл в HDFS
load_geo_csv = BashOperator(
    task_id="load_geo_csv",
    dag=dag_preparation,
    bash_command = "hdfs dfs -put -f /lessons/geo.csv /user/antodnv/geo.csv"
)

# Выполняем код скрипта для получения/записи семпла данных
data_sample_load = BashOperator(
    task_id="data_sample_load",
    dag=dag_preparation,
    bash_command = """spark-submit --master yarn --deploy-mode cluster \
                      --num-executors 2 --executor-memory 4g \
                      /lessons/preparation.py \
                      '/user/master/data/geo/events/' \
                      '/user/antodnv/data/geo/events/'
                    """
)

get_geo_csv >> load_geo_csv >> data_sample_load