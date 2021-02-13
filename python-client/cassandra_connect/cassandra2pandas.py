import pandas as pd
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.protocol import NumpyProtocolHandler, LazyProtocolHandler

CASSANDRA_USER = "reservation_service"
CASSANDRA_PASS = "i6XJsj!k#9"
CASSANDRA_HOST = "localhost"
CASSANDRA_PORT = "9042"
CASSANDRA_KEYSPACE = "reservation"
CASSANDRA_TABLE = "reservations_by_confirmation"

auth_provider = PlainTextAuthProvider(
    username=CASSANDRA_USER,
    password=CASSANDRA_PASS
)
cluster = Cluster(
    contact_points=[CASSANDRA_HOST],
    port=CASSANDRA_PORT,
    auth_provider=auth_provider
)


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


def read_keyspace_table(ks, tb):
    query = f"SELECT * FROM {ks}.{tb};"
    with cluster.connect(CASSANDRA_KEYSPACE) as session:
        session.default_fetch_size = None
        session.row_factory = pandas_factory
        rslt = session.execute(query, timeout=None)
    df = rslt._current_rows
    return df


def main():
    df = read_keyspace_table(CASSANDRA_KEYSPACE, CASSANDRA_TABLE)
    print(f"df.shape={df.shape}")
    df.to_csv("example.csv", index=False)


if __name__ == "__main__":
    main()
