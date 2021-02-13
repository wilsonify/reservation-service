import pandas as pd
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

CASSANDRA_USER = "reservation_service"
CASSANDRA_PASS = "i6XJsj!k#9"
CASSANDRA_HOST = "localhost"
CASSANDRA_PORT = "9042"

auth_provider = PlainTextAuthProvider(
    username=CASSANDRA_USER,
    password=CASSANDRA_PASS
)
cluster = Cluster(
    contact_points=[CASSANDRA_HOST],
    port=CASSANDRA_PORT,
    auth_provider=auth_provider
)


def write_keyspace_table(df, ks, tb):
    columns_string = ",".join(df.columns)
    question_string = ",".join(["?" for _ in df.columns])
    query = f"INSERT INTO {ks}.{tb}({columns_string}) VALUES ({question_string})"
    with cluster.connect(ks) as session:
        prepared = session.prepare(query)
        for item in df:
            session.execute(prepared, *item)


def main():
    df = pd.read_csv("../test/data/example.csv")
    print(f"df.shape={df.shape}")
    write_keyspace_table(df, "reservation", "reservations_by_confirmation")


if __name__ == "__main__":
    main()
