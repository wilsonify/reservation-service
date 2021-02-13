import time

import findspark
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName(
        "Spark Exploration App"
    ).config(
        'spark.jars.packages',
        'com.datastax.spark:spark-cassandra-connector_2.11:2.3.2'
    ).getOrCreate()

    df = spark.read.parquet("/PATH/TO/FILE/")

    df.write.format(
        "org.apache.spark.sql.cassandra"
    ).mode('append').options(
        table="few_com",
        keyspace="bmbr"
    ).save()


if __name__ == "__main__":
    findspark.init()
    start = time.time()
    main()
    end = time.time()
    print(f"end - start = {end - start}")
