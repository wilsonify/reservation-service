import findspark

from pyspark.sql import SparkSession

CASSANDRA_USER = "reservation_service"
CASSANDRA_PASS = "i6XJsj!k#9"
CASSANDRA_HOST = "localhost"
CASSANDRA_PORT = "9042"


def main():
    spark = SparkSession.builder.master(
        "spark://localhost:7077"
    ).appName(
        "Python Spark CQL basic example"
    ).config(
        'spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.0.0'
    ).config(
        "spark.cassandra.connection.host", CASSANDRA_HOST
    ).config(
        "spark.cassandra.connection.port", CASSANDRA_PORT
    ).config(
        "spark.cassandra.auth.username", CASSANDRA_USER
    ).config(
        "spark.cassandra.auth.password", CASSANDRA_PASS
    ).getOrCreate()

    df = spark.read.format(
        "org.apache.spark.sql.cassandra"
    ).option(
        "keyspace", "reservation"
    ).option(
        "table", "reservations_by_confirmation"
    ).load()
    df_shape = (df.count(), len(df.columns))
    print(f"df_shape={df_shape}")


if __name__ == "__main__":
    """
    spark-shell --packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.2 --conf spark.cassandra.connection.host=localhost
    """
    findspark.init()
    main()
