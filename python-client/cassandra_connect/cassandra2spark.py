import time
import findspark
import pandas as pd
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName(
        "Spark Exploration App"
    ).config(
        'spark.jars.packages',
        'com.datastax.spark:spark-cassandra-connector_2.11:2.3.2'
    ).getOrCreate()

    spark.read.format(
        "org.apache.spark.sql.cassandra"
    ).option(
        keyspace="ksname",
        table="tab"
    ).load()


if __name__ == "__main__":
    findspark.init()
    main()
