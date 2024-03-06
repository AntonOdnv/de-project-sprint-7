import sys
import os
import findspark
from pyspark.sql import SparkSession

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

findspark.init()
findspark.find()

def main():
    base_input_path=sys.argv[1]
    base_output_path=sys.argv[2]

    # Инициализируем Spark сессию
    spark = SparkSession \
                .builder \
                .master("yarn") \
                .appName("s7_project_antdodnv_load_sample") \
                .config("spark.executor.memory", "2g") \
                .config("spark.executor.cores", "2") \
                .config("spark.driver.cores", "2") \
                .config("spark.ui.port", "4051") \
                .getOrCreate()

    # Забираем 5% данных
    df_events = spark.read.parquet(f"{base_input_path}").sample(0.05)

    # Записываем данные. Мы каждй раз читаем случайные данные, поэтому просто перезаписываем текущий файл
    df_events.write\
             .partitionBy(['date', 'event_type'])\
             .format('parquet')\
             .save(f"{base_output_path}", mode='overwrite')

if __name__ == "__main__":
    main()