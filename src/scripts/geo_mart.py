import sys
import os
import findspark
import datetime
import math
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.window import Window

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

findspark.init()
findspark.find()

date = sys.argv[1]
depth = sys.argv[2]
base_input_path = sys.argv[3]
base_output_path = sys.argv[4]

# Функция расчета дистанции
def get_distance(lat_1, lat_2, lng_1, lng_2):
    lat_1=(math.pi / 180) * lat_1
    lat_2=(math.pi / 180) * lat_2
    lng_1=(math.pi / 180) * lng_1
    lng_2=(math.pi / 180) * lng_2
    return  2 * 6371 * math.asin(math.sqrt(math.pow(math.sin((lat_2 - lat_1) / 2), 2) +
    math.cos(lat_1) * math.cos(lat_2) * math.pow(math.sin((lng_2 - lng_1) / 2),2)))

# Заворачиваем в Spark функцию
udf_distance=F.udf(get_distance)


# Функция чтения путей файлов
def input_event_paths(date, depth):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [f"{base_input_path}date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(depth)]

def main():
    
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

    # Загружаем города из файла (использую сразу файл с таймзонами от преподавателя)
    # Google залочен, библиотеки не кластер поставить не дает... Поэтому хардкод
    cities = spark.read.csv("/user/antodnv/geo.csv", sep = ";", header = True) \
            .withColumn("lat", F.regexp_replace("lat", ",", ".").cast("double")) \
            .withColumn("lng", F.regexp_replace("lng", ",", ".").cast("double")) \
            .withColumnRenamed("lat", "lat_city") \
            .withColumnRenamed("lng", "lng_city") 

    # Читаем данные
    paths = input_event_paths(date, depth)
    events = spark.read.option("basePath", "/user/antodnv/data/geo/events/").parquet(*paths)

    def events_with_cities(events, type_of_event):
        if type_of_event == "user":
            _event_type = "message"
        else: _event_type = type_of_event
            
        spark_df = (events.filter(F.col("event_type")==_event_type).crossJoin(cities.hint("broadcast")) 
                    .filter((F.col("lat").isNotNull())&(F.col("lon").isNotNull())) 
                    .withColumn("dt", F.coalesce(F.col("event.datetime"), F.col("event.message_ts"))) 
                    .withColumn("week", F.weekofyear(F.col("dt"))) 
                    .withColumn("month", F.month(F.col("dt"))) 
                    .withColumn("distance", udf_distance(F.col("lat"), F.col("lat_city"), F.col("lon"), F.col("lng_city"))) 
                    .withColumn("distance_rank",
                                F.row_number().over(Window().partitionBy(["lat", "lon"]).orderBy("distance"))
                                ).where("distance_rank == 1")
                    .selectExpr("event.message_from as user_id", "dt", "id as zone_id", "week", "month")
                    .withColumn(f"week_{type_of_event}",  F.count("*").over(Window.partitionBy("zone_id", "week"))) 
                    .withColumn(f"month_{type_of_event}", F.count("*").over(Window.partitionBy("zone_id", "month"))) 
                    .distinct()
                    )
        if type_of_event == "user":
            spark_df = (spark_df
                        .withColumn("row", 
                                    F.row_number().over(Window.partitionBy("user_id").orderBy(F.col("dt").asc()))
                                ).where("row == 1")
                        .selectExpr("zone_id", "week", "month")
                        .withColumn(f"week_{type_of_event}",  F.count("*").over(Window.partitionBy("zone_id", "week"))) 
                        .withColumn(f"month_{type_of_event}", F.count("*").over(Window.partitionBy("zone_id", "month"))) 
                        .distinct()
                        )
        else:
            spark_df = (spark_df
                        .selectExpr("zone_id", "week", "month")
                        .withColumn(f"week_{type_of_event}",  F.count("*").over(Window.partitionBy("zone_id", "week"))) 
                        .withColumn(f"month_{type_of_event}", F.count("*").over(Window.partitionBy("zone_id", "month"))) 
                        .distinct()
                        )
        return spark_df
    
    # Обрабатываем каждый тип отдельно, чтобы не перегружать компьют    
    messages = events_with_cities(events, "message")
    reactions = events_with_cities(events, "reaction")
    subscriptions = events_with_cities(events, "subscription")
    users = events_with_cities(events, "user")

    # Объединяем все в общую витрину
    geo_mart = (messages.join(reactions, ["zone_id", "week", "month"], how="full")
                .join(subscriptions, ["zone_id", "week", "month"], how="full")
                .join(users, ["zone_id", "week", "month"], how="full")
                ).fillna(0)

    # Пишем результат в hdfs
    geo_mart.write.mode("overwrite").parquet(
        f"{base_output_path}/mart/geo_mart/_{date}_{depth}"
    )        
    
if __name__ == "__main__":
    main()
