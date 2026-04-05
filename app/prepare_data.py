import os
import re
import shutil
from settings import (
    DATA_DIR,
    HDFS_INPUT_PATH,
    SAMPLE_LIMIT,
    HDFS_PARQUET_PATH,
    SPARK_APP_NAME_PREP,
)
from common import create_spark_session, get_hdfs_filesystem, delete_hdfs_path


def clean_data_directory():
    if os.path.isdir(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    os.mkdir(DATA_DIR)


def sample_data(df, limit):
    count = df.count()
    fraction = min(1.0, (2 * limit) / count)
    return (
        df.select(["id", "title", "text"])
        .sample(fraction=fraction, seed=0)
        .limit(limit)
    )


def save_row_as_file(row):
    title = str(row["title"]).replace(" ", "_")
    clean_title = re.sub(r"[^A-Za-z0-9_]", "", title)
    filename = f"{DATA_DIR}/{row['id']}_{clean_title}.txt"
    text = str(row["text"]).replace("\n", " ").replace("\r", " ").replace("\t", " ")
    with open(filename, "w", encoding="utf-8") as f:
        f.write(text)


def transform_file(file_path_text):
    path, text = file_path_text
    name = path.split("/")[-1].replace(".txt", "")
    parts = name.split("_", 1)
    doc_id = parts[0]
    header = parts[1] if len(parts) > 1 else "Unknown"
    return f"{doc_id}\t{header}\t{text}"


def main():
    clean_data_directory()

    spark = create_spark_session(SPARK_APP_NAME_PREP)
    context = spark.sparkContext

    df = spark.read.parquet(HDFS_PARQUET_PATH)
    sample = sample_data(df, SAMPLE_LIMIT)

    sample.foreach(save_row_as_file)

    rdd = context.wholeTextFiles("file://" + os.path.abspath(DATA_DIR) + "/*.txt")
    transformed = rdd.map(transform_file).coalesce(1)

    fs, Path = get_hdfs_filesystem(context)
    delete_hdfs_path(fs, Path, HDFS_INPUT_PATH)

    transformed.saveAsTextFile("hdfs:///" + HDFS_INPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
