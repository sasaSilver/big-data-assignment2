import re
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from settings import (
    CASSANDRA_HOST,
    CASSANDRA_PORT,
)


def get_cassandra_connection(keyspace=None):
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(keyspace) if keyspace else cluster.connect()
    return cluster, session


def create_spark_session(app_name):
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .getOrCreate()
    )


def extract_terms(text):
    return re.findall(r"[a-z0-9]+", text.lower())


def get_hdfs_filesystem(context):
    URI = context._gateway.jvm.java.net.URI
    FileSystem = context._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Path = context._gateway.jvm.org.apache.hadoop.fs.Path
    fs = FileSystem.get(URI("hdfs:///"), context._jsc.hadoopConfiguration())
    return fs, Path


def delete_hdfs_path(fs, Path, path):
    if fs.exists(Path(path)):
        fs.delete(Path(path), True)
