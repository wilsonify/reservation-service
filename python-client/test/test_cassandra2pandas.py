import cassandra_connect
from cassandra_connect import cassandra2pandas


def test_smoke():
    print("is anything on fire?")


def test_read_keyspace_table():
    df = cassandra2pandas.read_keyspace_table(CASSANDRA_KEYSPACE, CASSANDRA_TABLE)
    assert df.shape == (1, 1)
