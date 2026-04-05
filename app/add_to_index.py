import sys
from collections import Counter
from common import get_cassandra_connection, extract_terms


def parse_filepath(filepath):
    filename = filepath.split("/")[-1].replace(".txt", "")
    doc_id = filename.split("_")[0]
    return filename, doc_id


def read_file(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return f.read().lower()


def update_total_docs(session):
    row = session.execute("SELECT value FROM global_data WHERE key='DOCS_COUNT'").one()
    new_value = row.value + 1 if row else 1
    session.execute(
        "INSERT INTO global_data (key, value) VALUES ('DOCS_COUNT', %s)", (new_value,)
    )


def update_document_length(session, doc_id, length):
    session.execute(
        "INSERT INTO dl (doc_id, length) VALUES (%s, %s)",
        (doc_id, length),
    )


def update_term_frequencies(session, doc_id, tf_counts):
    for term, tf in tf_counts.items():
        row = session.execute(
            "SELECT df FROM df WHERE term=%s", (term,)
        ).one()
        new_df = row.df + 1 if row else 1
        session.execute(
            "INSERT INTO df (term, df) VALUES (%s, %s)",
            (term, new_df),
        )
        session.execute(
            "INSERT INTO tf (term, doc_id, tf) VALUES (%s, %s, %s)",
            (term, doc_id, tf),
        )


def main():
    filepath = sys.argv[1]
    filename, doc_id = parse_filepath(filepath)

    text = read_file(filepath)
    words = extract_terms(text)
    doc_length = len(words)
    tf_counts = Counter(words)

    cluster, session = get_cassandra_connection()

    update_total_docs(session)
    update_document_length(session, doc_id, doc_length)
    update_term_frequencies(session, doc_id, tf_counts)

    print(f"File {filename} indexed in ScyllaDB successfully.")
    cluster.shutdown()


if __name__ == "__main__":
    main()
