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

    # Найдем уникальные пары пользователей, которые подписаны на один канал
    # Сначала соберем датасет со всеми подписками всех пользователей
    user_subscriptions = (events.filter(F.col("event_type") == "subscription") 
        .filter(F.col("event.subscription_channel").isNotNull() & F.col("event.user").isNotNull()) 
        
        .selectExpr("event.subscription_channel as channel_id", "event.user as user_id") 
        .distinct() )
    
    # Нас будут интересовать только координаты самой актуальной активности пользователей для вычисления расстояний близости 1км
    # Отберем актуальные координаты для каждого пользователя по аналогии с определеинем active_city в geo_mart
    users_active_location = (events.filter(F.col("event_type") == "message") 
        .withColumn("dt", F.coalesce(F.col("event.datetime"), F.col("event.message_ts"))) 
        .withColumn("datetime_rank",
                    F.row_number().over(Window().partitionBy(["event.message_from"]).orderBy(F.desc("dt"))) 
        ).where("datetime_rank == 1") 
        .selectExpr("event.message_from as user_id", "lat", "lon") )

    # Добавим к user_subscriptions актуальные кооординаты пользователей
    # Их координаты должны существовать, иначе не проверить условие в 1 км между пользователямию Поэтому "inner"
    user_subscriptions_joined = (user_subscriptions
        .join(users_active_location, on="user_id", how="inner") 
        .selectExpr("user_id", "channel_id", "lat", "lon")
        .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull()) )

    # Затем выберем найдем все пары подьзователей, подписанных на 1 канал. 
    # Для каждого пользователя соберем все возможные сочетания пар других пользователей с той же подпиской.
    # У нас определенно есть дубли, а нужны уникальные пары. 
    # Среди двух id один точно меньше по номеру. Воспользуемся этим, чтобы отсечь лишних.
    user_pairs_unique = (user_subscriptions_joined 
        .selectExpr("user_id as user_left", "lat as lat_left", "lon as lon_left", "channel_id") 
        .join(user_subscriptions_joined.selectExpr("user_id as user_right", "lat as lat_right", "lon as lon_right", "channel_id"), 
              on="channel_id", how="inner") 
        .drop("channel_id") 
        .filter("user_left < user_right") 
        .withColumn("user_distance", 
                    udf_distance(F.col("lat_left"), F.col("lat_right"), F.col("lon_left"), F.col("lon_right"))) 
        .filter(F.col("user_distance") <= 1.0) 
        .crossJoin(cities.hint("broadcast")) 
        .withColumn("distance", 
                    udf_distance(F.col("lat_left"), F.col("lat_city"), F.col("lon_left"), F.col("lng_city"))) 
        .withColumn("distance_rank",
                    F.row_number().over(Window().partitionBy(["user_left", "user_right"]).orderBy("distance"))
                    ).where("distance_rank == 1") 
        .selectExpr("user_left", "user_right", "id as zone_id", "timezone") 
        .distinct() )
    
     # Найдем уникальные пары пользователей, которые переписывались. Сначала в одну сторону
    user_senders = (events.filter(F.col("event_type") == "message") 
        .filter(F.col("event.message_from").isNotNull() & F.col("event.message_to").isNotNull()) 
        .selectExpr("event.message_from as user_left", "event.message_to as user_right") 
        .distinct() )

    # Найдем уникальные пары пользователей, которые переписывались. 
    # Теперь разворачиваем пары - получатель становится отправителем
    user_recievers = (events.filter(F.col("event_type") == "message") 
        .filter(F.col("event.message_from").isNotNull() & F.col("event.message_to").isNotNull()) 
        .selectExpr("event.message_to as user_left", "event.message_from as user_right") 
        .distinct() )

    # Соединяем и отюираем уникальных по принципу как выше - отправитель с меньшим номером
    contacted_users = user_senders.union(user_recievers).filter("user_left < user_right") 

    # Теперь нам нужно из отобранных пользователей с общими каналами на расстоянии не более 1км 
    # убрать пользователей, которые уже имели общие сообщения. Используем объединение "left_anti".
    # Затем добавим к итоговой витрине дату/время сборки данных - текущую на момент записи +
    # + добавим колону даты/времени для конкретной временной зоны.
    # Оставляю именно дату/время, т.к. только времени недостаточно.
    # Если я захочу узнать, витрина обновилась в 2:30 по локальному времени уже сегодня или только вчера? Нужна дата.
    recommendations_mart = (user_pairs_unique.join(contacted_users, on=["user_left", "user_right"], how="left_anti") 
                           .withColumn("processed_dttm", current_timestamp()) 
                           .withColumn("local_time", 
                                       F.from_utc_timestamp(F.col("processed_dttm"), F.col("timezone"))) 
                           .selectExpr("user_left", "user_right", "processed_dttm", "zone_id", "local_time") )

    # Пишем результат в hdfs
    recommendations_mart.write.mode("overwrite").parquet(
        f"{base_output_path}/mart/recommendations_mart/_{date}_{depth}"
    )        
    
if __name__ == "__main__":
    main()
