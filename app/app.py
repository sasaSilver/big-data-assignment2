from settings import KEYSPACE, INDEX_FILE, BATCH_SIZE
from common import get_cassandra_connection


def create_keyspace(conn):
    conn.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
    """)


def create_tables(conn):
    conn.set_keyspace(KEYSPACE)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS global_data(
            key text PRIMARY KEY,
            value int
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dl(
            doc_id text PRIMARY KEY,
            length int
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS df(
            term text PRIMARY KEY,
            df int
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tf(
            term text,
            doc_id text,
            tf int,
            PRIMARY KEY (term, doc_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS docs(
            doc_id text PRIMARY KEY,
            title text
        )
    """)


def prepare_queries(conn):
    return {
        "global": conn.prepare("""
            INSERT INTO global_data (key, value)
            VALUES (?, ?)
        """),
        "dl": conn.prepare("""
            INSERT INTO dl (doc_id, length)
            VALUES (?, ?)
        """),
        "df": conn.prepare("""
            INSERT INTO df (term, df)
            VALUES (?, ?)
        """),
        "tf": conn.prepare("""
            INSERT INTO tf (term, doc_id, tf)
            VALUES (?, ?, ?)
        """),
        "title": conn.prepare("""
            INSERT INTO docs (doc_id, title)
            VALUES (?, ?)
        """),
    }


def process_entry(key, value, queries, conn):
    prefix = key[:3]

    if key == "DOCS_COUNT":
        num = int(value)
        conn.execute(queries["global"], (key, num))
    elif prefix == "TF:":
        rest = key[3:]
        term, doc_id = rest.split("@", 1)
        num = int(value)
        conn.execute_async(queries["tf"], (term, doc_id, num))
    elif prefix == "DF:":
        term = key[3:]
        num = int(value)
        conn.execute_async(queries["df"], (term, num))
    elif prefix == "DL:":
        doc_id = key[3:]
        num = int(value)
        conn.execute_async(queries["dl"], (doc_id, num))
    elif prefix == "TT:":
        doc_id = key[3:]
        title = value
        conn.execute_async(queries["title"], (doc_id, title))


def load_index_file(filename, queries, conn):
    count = 0
    with open(filename, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            row = line.strip().split("\t")
            if len(row) != 2:
                continue

            key, value = row
            process_entry(key, value, queries, conn)
            count += 1

            if (i + 1) % BATCH_SIZE == 0:
                print(f"Loaded {count} entries")


def main():
    client, conn = get_cassandra_connection()

    create_keyspace(conn)
    conn.set_keyspace(KEYSPACE)
    create_tables(conn)
    queries = prepare_queries(conn)

    load_index_file(INDEX_FILE, queries, conn)
    
    conn.shutdown()
    client.shutdown()


if __name__ == "__main__":
    main()
