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

    # Собираем данные по сообщениям с городами и прочими атрибутами по ТЗ
    messages_with_cities_and_tz = (events.where(F.col("event_type") == "message")
        # Докидываем города
        .crossJoin(cities.hint("broadcast")) 
        # Вычисляем расстояния
        .withColumn("distance", udf_distance(F.col("lat"), F.col("lat_city"), F.col("lon"), F.col("lng_city")))  
        # Берем ближайший
        .withColumn("distance_rank",
                    F.row_number().over(Window().partitionBy(["event.message_from", "event.message_id"]).orderBy("distance"))
                    ).where("distance_rank == 1") 
        .withColumn("message_date",
                    F.date_trunc("day", F.col("event.message_ts"))) 
        # Оставляем только отправленные
        .filter(F.col('event.message_ts').isNotNull()) 
        # Считаем local time
        .withColumn("localtime", 
                    F.from_utc_timestamp(F.col("event.message_ts"), F.col("timezone"))) 
        .selectExpr("event.message_from as user_id", "event.message_id", "message_date", 
                    "event.message_to", "event.message_from", "event.message_ts", "city", "localtime", "timezone") )

    # Вычисляем активный город. Это город последней отправки сообщения
    users_act_city = (messages_with_cities_and_tz 
        .withColumn("datetime_rank",
                    F.row_number().over(Window().partitionBy(["user_id"]).orderBy(F.desc("message_ts"))) 
        ).where("datetime_rank == 1") 
        .orderBy("user_id") 
        .selectExpr("user_id", "city as act_city", "message_date", "message_ts", "timezone") )

    # Собираем датасет с изменениями города по каждому пользователю
    city_lags = (messages_with_cities_and_tz 
        .withColumn("max_date", 
                    F.max("message_date").over(Window().partitionBy("user_id"))) 
        .withColumn("city_lag",
                    F.lead("city", 1, "-").over(Window().partitionBy("user_id").orderBy(F.col("message_ts").desc()))) 
        .filter((F.col("city") != F.col("city_lag")) & (F.col("city_lag") != '-')) 
        .select("user_id", "message_date", "city", "timezone", "max_date", "city_lag") )

    # Определяем домашний город. Это последний город, в котором пользователь был дольше 27 дней
    users_home_city = (city_lags 
        .withColumn( "date_lag",
                    F.coalesce(F.lag("message_date")
                        .over( Window().partitionBy("user_id").orderBy(F.col("message_date").desc())),
                        F.col("max_date"))) 
        .withColumn("date_delta", F.datediff(F.col("date_lag"), F.col("message_date"))) 
        .filter(F.col("date_delta") > 27) # берем данные только по городам, где дельта в днях более 27
        .withColumn("date_rank",
                    F.row_number().over(Window.partitionBy("user_id").orderBy(F.col("message_date").desc()))) 
        .where(F.col("date_rank") == 1) # Берем самый свежий домашний город
        .selectExpr("user_id", "city as home_city") )

    # Собираем датасет с посещенными пользователем городами и их количеством
    travel_list = city_lags.groupBy("user_id").agg(
                F.count("*").alias("travel_count"),
                F.collect_list("city").alias("travel_array"))

    # Объединяем данные, собираем витрину
    users_mart = (messages_with_cities_and_tz 
        .join(users_act_city, "user_id", "left") 
        .join(users_home_city, "user_id", "left") 
        .join(travel_list, "user_id", "left") 
        .select("user_id", "act_city", "home_city", "travel_count", "travel_array", "localtime") )
    
    # Пишем результат в hdfs
    users_mart.write.mode("overwrite").parquet(
        f"{base_output_path}/mart/users_mart/_{date}_{depth}"
    )
    
if __name__ == "__main__":
    main()
