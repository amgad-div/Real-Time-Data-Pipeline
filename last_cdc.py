
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from py4j.java_gateway import java_import
import datetime

# Spark Session
spark = SparkSession.builder.appName("CDC Delta Fix") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Paths
source_path = "hdfs://namenode:8020/amgad/*"
source_dir = "hdfs://namenode:8020/amgad/"
archive_dir = "hdfs://namenode:8020/amgad/archive/"
delta_output_path = "hdfs://namenode:8020/amgad/books_delta"

#  Prepare FileSystem
sc = spark.sparkContext
java_import(sc._gateway.jvm, "org.apache.hadoop.fs.*")
java_import(sc._gateway.jvm, "java.net.URI")
fs = sc._jvm.FileSystem.get(sc._jvm.URI("hdfs://namenode:8020"), sc._jsc.hadoopConfiguration())
srcPath = sc._jvm.Path(source_dir)
archivePath = sc._jvm.Path(archive_dir)

#  List current files before processing
print("\n Current files in /amgad/:")
status_list = fs.listStatus(srcPath)
for status in status_list:
    name = status.getPath().getName()
    if status.isFile() and not name.startswith("books_"):
        mod_time = datetime.datetime.fromtimestamp(status.getModificationTime() / 1000)
        print(f"📎 {name} (modified: {mod_time})")

#  Load raw data
df_raw = spark.read.json(source_path)

#  Extract upserts
df_upserts = df_raw.filter(
    (col("payload.after").isNotNull()) & (col("payload.op").isin("c", "u"))
).selectExpr("payload.after.*", "payload.op as op").withColumn("id", col("id").cast("int"))

# Extract deletes directly from Struct
df_deletes = df_raw.filter(
    (col("payload.after").isNull()) & (col("payload.op") == "d")
).select(
    col("payload.before.id").cast("int").alias("id"),
    col("payload.op").alias("op")
)

#  Show CDC data
if df_upserts.count() > 0:
    print("\n New Inserts/Updates:")
    df_upserts.select("id", "title", "category", "op").show(truncate=False)

if df_deletes.count() > 0:
    print("\nNew Deletes:")
    df_deletes.show(truncate=False)

#  Delta update
try:
    delta_table = DeltaTable.forPath(spark, delta_output_path)
    print("\n Delta Table loaded")
    delta_table.alias("old").merge(
        df_upserts.alias("new"),
        "old.id = new.id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    ids_to_delete = [row["id"] for row in df_deletes.collect()]
    if ids_to_delete:
        print("\n Rows to delete:")
        delta_table.toDF().filter(col("id").isin(ids_to_delete)).show(truncate=False)

        for id in ids_to_delete:
            delta_table.delete(col("id") == id)

        print("\n Deleted rows:")
        delta_table.toDF().filter(col("id").isin(ids_to_delete)).show(truncate=False)

except AnalysisException:
    if df_upserts.count() > 0:
        df_upserts.write.format("delta").mode("overwrite").save(delta_output_path)
        print("\nDelta Table created with initial upserts.")

#  Summary
inserted = df_upserts.filter(col("op") == "c").count()
updated = df_upserts.filter(col("op") == "u").count()
deleted = df_deletes.count()
print(f"\nSummary → Inserted: {inserted}, Updated: {updated}, Deleted: {deleted}")

#  Preview and move files to archive
print("\n Previewing and archiving files...")

if not fs.exists(archivePath):
    fs.mkdirs(archivePath)

for status in status_list:
    name = status.getPath().getName()
    if status.isFile() and not name.startswith("books_"):
        try:
            file_path = f"hdfs://namenode:8020/amgad/{name}"
            print(f"\n Preview of file: {name}")
            spark.read.json(file_path).show(truncate=False)

            dest = sc._jvm.Path(archivePath.toString() + "/" + name)
            print(f" Moving {name} → archive/")
            fs.rename(status.getPath(), dest)
        except Exception as e:
            print(f" Error processing {name}: {e}")

#  Stop Spark
spark.stop()