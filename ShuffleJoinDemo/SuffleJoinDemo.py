from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, broadcast

from lib.logger import Log4j


def broardcast(flight_time_df2):
    pass


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Spark Join Demo") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)

    flight_time_df1 = spark.read.json("data/d1/")
    flight_time_df2 = spark.read.json("data/d2/")

    spark.conf.set("spark.sql.shuffle.partition", 3)

    join_expr = flight_time_df1.id == flight_time_df2.id
    join_df = flight_time_df1.join(broadcast(flight_time_df2), join_expr, "inner")

    join_df.foreach(lambda f: None)
    input("press a key to stop ...")