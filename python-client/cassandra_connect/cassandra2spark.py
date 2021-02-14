import findspark
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName(
        "Spark Exploration App"
    ).config(
        'spark.jars.packages',
        'com.datastax.spark:spark-cassandra-connector_2.11:2.3.2'
    ).getOrCreate()

    df = spark.read.format(
        "org.apache.spark.sql.cassandra"
    ).option(
        keyspace="reservation",
        table="reservations_by_confirmation"
    ).load()
    df_shape = (df.count(), len(df.columns))
    print(f"df_shape={df_shape}")


if __name__ == "__main__":
    findspark.init()
    main()
