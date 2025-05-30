import pyspark
from pyspark.sql import SparkSession
import os

from pyspark.sql.functions import spark_partition_id
from pyspark.sql.types import *

from lib.logger import Log4j


print(f"PySpark version: {pyspark.__version__}")

if __name__ == "__main__":
    current_dir = os.getcwd()
    log4j_path = f"file:{current_dir}/log4j.properties"

    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.1") \
        .config("spark.jars.repositories", "https://repo1.maven.org/maven2/") \
        .getOrCreate()

    logger = Log4j(spark)

    flightParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/flight*.parquet")

    logger.info("NUm Partitions before:" + str(flightParquetDF.rdd.getNumPartitions()))
    flightParquetDF.groupBy(spark_partition_id()).count().show()

    partitionedDF = flightParquetDF.repartition(5)
    logger.info("NUm Partitions after:" + str(flightParquetDF.rdd.getNumPartitions()))
    partitionedDF.groupBy(spark_partition_id()).count().show()

    """
    partitionedDF.write \
        .format("avro") \
        .mode('overwrite') \
        .option("path", "dataSink/avro/") \
        .save()
    """

    flightParquetDF.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "dataSink/json/") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerfile", 10000) \
        .save()