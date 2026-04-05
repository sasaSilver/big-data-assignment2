import sys
from collections import Counter
import re

def extract_terms(text):
    return re.findall(r"[a-z0-9]+", text.lower())


def parse_line(line):
    fields = line.strip().split("\t")
    if len(fields) != 3:
        return None
    return fields


def process_document(doc_id, title, text):
    words = extract_terms(text)
    length = len(words)

    if length == 0:
        return

    print(f"TT:{doc_id}\t{title}")
    print(f"DL:{doc_id}\t{length}")
    print("DOCS_COUNT\t1")

    counter = Counter(words)
    for term, count in counter.items():
        print(f"TF:{term}@{doc_id}\t{count}")
        print(f"DF:{term}\t1")


def main():
    for line in sys.stdin:
        parsed = parse_line(line)
        if not parsed:
            continue

        doc_id, title, text = parsed
        process_document(doc_id, title, text)


if __name__ == "__main__":
    main()
