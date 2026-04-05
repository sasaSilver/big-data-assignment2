import sys
import re
import math
from pyspark.sql import SparkSession
from settings import KEYSPACE, K1, B, TOP_K, SPARK_APP_NAME_RANKER
from common import get_cassandra_connection


def get_total_docs(conn):
    row = conn.execute("SELECT value FROM global_data WHERE key='DOCS_COUNT'").one()
    return row.value if row else 0


def get_doc_lengths(conn):
    rows = conn.execute("SELECT doc_id, length FROM dl")
    lengths = {row.doc_id: row.length for row in rows}
    total = sum(lengths.values())
    avg = total / len(lengths) if lengths else 1
    return lengths, avg


def calculate_bm25(tf, idf, dl, avg_dl):
    return idf * ((tf * (K1 + 1)) / (tf + K1 * (1 - B + B * (dl / avg_dl))))


def process_term(conn, term, total, doc_lengths, avg_dl):
    df_row = conn.execute("SELECT df FROM df WHERE term=%s", (term,)).one()
    if not df_row:
        return []

    idf = math.log(total / df_row.df)
    tf_rows = conn.execute("SELECT doc_id, tf FROM tf WHERE term=%s", (term,))

    scores = []
    for tf_row in tf_rows:
        doc_id = tf_row.doc_id
        tf = tf_row.tf
        dl = doc_lengths.get(doc_id, avg_dl)
        score = calculate_bm25(tf, idf, dl, avg_dl)
        scores.append((doc_id, score))

    return scores


def get_titles(conn, doc_ids):
    if not doc_ids:
        return {}
    
    placeholders = ", ".join(["%s"] * len(doc_ids))
    query = f"SELECT doc_id, title FROM docs WHERE doc_id IN ({placeholders})"
    
    rows = conn.execute(query, list(doc_ids))
    return {row.doc_id: row.title for row in rows}


def print_results(query, top, title_map):
    print("\n" + "=" * 60)
    print(f"Results for: '{query}'")
    print("=" * 60)
    for i, (doc_id, score) in enumerate(top, 1):
        title = title_map.get(doc_id, "Unknown").replace("_", " ")
        print(f"{i}. [{score:.4f}] {doc_id} | {title}")
    print("=" * 60 + "\n")


def main():
    spark = SparkSession.builder.appName(SPARK_APP_NAME_RANKER).getOrCreate()
    context = spark.sparkContext

    query = " ".join(sys.argv[1:]).lower()
    terms = re.findall(r"[a-z0-9]+", query)

    cluster, conn = get_cassandra_connection(KEYSPACE)

    total = get_total_docs(conn)
    doc_lengths, avg_dl = get_doc_lengths(conn)

    pairs = []
    for term in terms:
        pairs.extend(process_term(conn, term, total, doc_lengths, avg_dl))

    if not pairs:
        print("\n" + "=" * 60)
        print(f"\nNo results: {query}\n")
        print("=" * 60)
        cluster.shutdown()
        spark.stop()
        sys.exit(0)

    prdd = context.parallelize(pairs)
    sums = prdd.reduceByKey(lambda a, b: a + b)

    top = sums.sortBy(lambda x: x[1], ascending=False).take(TOP_K)
    doc_ids = {doc_id for doc_id, _ in top}

    title_map = get_titles(conn, doc_ids)
    print_results(query, top, title_map)

    cluster.shutdown()
    spark.stop()


if __name__ == "__main__":
    main()
