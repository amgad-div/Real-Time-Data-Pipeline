from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import pandas as pd
import dash
from dash import dcc, html, dash_table
import plotly.express as px
from py4j.java_gateway import java_import
import datetime

spark = SparkSession.builder.appName("DeltaDashboard").config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020").getOrCreate()

sc = spark.sparkContext
java_import(sc._gateway.jvm, "org.apache.hadoop.fs.*")
java_import(sc._gateway.jvm, "java.net.URI")

fs = sc._jvm.FileSystem.get(sc._jvm.java.net.URI("hdfs://namenode:8020"), sc._jsc.hadoopConfiguration())
archivePath = sc._jvm.Path("/amgad/archive")

# get last updated file 
latest_file = None
latest_time = 0
for status in fs.listStatus(archivePath):
    if status.isFile():
        mod_time = status.getModificationTime()
        if mod_time > latest_time:
            latest_time = mod_time
            latest_file = status.getPath().toString()

# readit 
try:
    df_latest = spark.read.json(latest_file)
    df_latest_flat = df_latest.selectExpr("payload.after.*", "payload.op") \
                              .withColumn("id", col("id").cast("int"))
    df_latest_pd = df_latest_flat.toPandas()
    latest_filename = latest_file.split("/")[-1]
except:
    df_latest_pd = pd.DataFrame(columns=["id", "title", "category", "op"])
    latest_filename ="not found "

# read all archive file
try:
    df_archive = spark.read.json("hdfs://namenode:8020/amgad/archive/*.*")
    df_archive_flat = df_archive.selectExpr("payload.after.*", "payload.op") \
                                .withColumn("id", col("id").cast("int"))
    df_archive_pd = df_archive_flat.toPandas()
except:
    df_archive_pd = pd.DataFrame(columns=["id", "title", "category", "op"])

# Delta Table
try:
    df_log = spark.read.json("hdfs://namenode:8020/amgad/books_delta/_delta_log/*.json")

    df_add = df_log.filter(col("add").isNotNull()).select(
        col("add.path").alias("path"),
        col("add.size").alias("size"),
        col("add.modificationTime").alias("modificationTime"),
        col("commitInfo.timestamp").alias("timestamp")
    ).withColumn("operation", lit("add"))

    df_remove = df_log.filter(col("remove").isNotNull()).select(
        col("remove.path").alias("path"),
        col("remove.deletionTimestamp").alias("modificationTime"),
        col("commitInfo.timestamp").alias("timestamp")
    ).withColumn("size", lit(None)).withColumn("operation", lit("remove"))

    df_combined = df_add.unionByName(df_remove)
    df_log_pd = df_combined.toPandas()
    df_log_pd["timestamp"] = pd.to_datetime(df_log_pd["timestamp"], unit="ms")
    df_log_pd["modificationTime"] = pd.to_datetime(df_log_pd["modificationTime"], unit="ms", errors="coerce")
    df_log_pd.sort_values("timestamp", inplace=True)

except:
    df_log_pd = pd.DataFrame(columns=["path", "size", "modificationTime", "timestamp", "operation"])

#  Dash
app = dash.Dash(__name__)
app.title = "Delta + Archive Monitor"

app.layout = html.Div([
    html.H2(" last edit : " + latest_filename, style={"textAlign": "center"}),

    dash_table.DataTable(
        data=df_latest_pd.to_dict("records"),
        columns=[{"name": i, "id": i} for i in df_latest_pd.columns],
        page_size=10,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left", "padding": "5px"},
        style_header={"backgroundColor": "#1abc9c", "color": "white", "fontWeight": "bold"}
    ),

    html.H2(" all archived file", style={"textAlign": "center", "marginTop": "40px"}),

    dash_table.DataTable(
        data=df_archive_pd.to_dict("records"),
        columns=[{"name": i, "id": i} for i in df_archive_pd.columns],
        page_size=15,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left", "padding": "5px"},
        style_header={"backgroundColor": "#2c3e50", "color": "white", "fontWeight": "bold"}
    ),

    html.H2(" Delta changed ", style={"textAlign": "center", "marginTop": "40px"}),

    dcc.Graph(
        figure=px.pie(df_log_pd, names="operation", title=" add vs delete")
    ),

    html.H4("record history", style={"textAlign": "center", "marginTop": "30px"}),

    dash_table.DataTable(
        data=df_log_pd.to_dict("records"),
        columns=[{"name": i, "id": i} for i in df_log_pd.columns],
        page_size=15,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left", "padding": "5px"},
        style_header={"backgroundColor": "#007acc", "color": "white", "fontWeight": "bold"}
    )
])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050)

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, lit
# import pandas as pd
# import dash
# from dash import dcc, html, dash_table
# import plotly.express as px
# from py4j.java_gateway import java_import

# # 🔧 إعداد Spark
# spark = SparkSession.builder \
#     .appName("DeltaDashboard") \
#     .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
#     .getOrCreate()

# # 🧠 إعداد FileSystem API
# sc = spark.sparkContext
# java_import(sc._gateway.jvm, "org.apache.hadoop.fs.*")
# java_import(sc._gateway.jvm, "java.net.URI")

# fs = sc._jvm.FileSystem.get(
#     sc._jvm.java.net.URI("hdfs://namenode:8020"),
#     sc._jsc.hadoopConfiguration()
# )
# archivePath = sc._jvm.Path("/amgad/archive")

# # 📁 تحديد أحدث ملف في الأرشيف
# latest_file = None
# latest_time = 0
# for status in fs.listStatus(archivePath):
#     if status.isFile():
#         mod_time = status.getModificationTime()
#         if mod_time > latest_time:
#             latest_time = mod_time
#             latest_file = status.getPath().toString()

# # 📥 قراءة أحدث ملف فقط
# try:
#     df_latest = spark.read.json(latest_file)

#     df_upserts = df_latest.filter(
#         (col("payload.after").isNotNull()) & (col("payload.op").isin("c", "u"))
#     ).selectExpr("payload.after.*", "payload.op").withColumn("id", col("id").cast("int"))

#     df_deletes = df_latest.filter(
#         (col("payload.after").isNull()) & (col("payload.op") == "d")
#     ).selectExpr("payload.before.id as id", "'deleted' as title", "'deleted' as category", "payload.op")

#     df_latest_flat = df_upserts.unionByName(df_deletes)
#     df_latest_pd = df_latest_flat.toPandas()
#     latest_filename = latest_file.split("/")[-1]
# except:
#     df_latest_pd = pd.DataFrame(columns=["id", "title", "category", "op"])
#     latest_filename = "غير متوفر"

# # 📂 قراءة جميع ملفات الأرشيف
# try:
#     df_all = spark.read.json("hdfs://namenode:8020/amgad/archive/*.json")

#     upserts = df_all.filter(
#         (col("payload.after").isNotNull()) & (col("payload.op").isin("c", "u"))
#     ).selectExpr("payload.after.*", "payload.op").withColumn("id", col("id").cast("int"))

#     deletes = df_all.filter(
#         (col("payload.after").isNull()) & (col("payload.op") == "d")
#     ).selectExpr("payload.before.id as id", "'deleted' as title", "'deleted' as category", "payload.op")

#     df_all_flat = upserts.unionByName(deletes)
#     df_archive_pd = df_all_flat.toPandas()
# except:
#     df_archive_pd = pd.DataFrame(columns=["id", "title", "category", "op"])

# # 📊 قراءة سجل Delta Table
# try:
#     df_log = spark.read.json("hdfs://namenode:8020/amgad/books_delta/_delta_log/*.json")

#     df_add = df_log.filter(col("add").isNotNull()).select(
#         col("add.path").alias("path"),
#         col("add.size").alias("size"),
#         col("add.modificationTime").alias("modificationTime"),
#         col("commitInfo.timestamp").alias("timestamp")
#     ).withColumn("operation", lit("add"))

#     df_remove = df_log.filter(col("remove").isNotNull()).select(
#         col("remove.path").alias("path"),
#         col("remove.deletionTimestamp").alias("modificationTime"),
#         col("commitInfo.timestamp").alias("timestamp")
#     ).withColumn("size", lit(None)).withColumn("operation", lit("remove"))

#     df_log_flat = df_add.unionByName(df_remove)
#     df_log_pd = df_log_flat.toPandas()
#     df_log_pd["timestamp"] = pd.to_datetime(df_log_pd["timestamp"], unit="ms")
#     df_log_pd["modificationTime"] = pd.to_datetime(df_log_pd["modificationTime"], unit="ms", errors="coerce")
#     df_log_pd.sort_values("timestamp", inplace=True)
# except:
#     df_log_pd = pd.DataFrame(columns=["path", "size", "modificationTime", "timestamp", "operation"])

# # 🎨 تصميم Dash UI
# app = dash.Dash(__name__)
# app.title = "Delta + Archive Dashboard"

# app.layout = html.Div([
#     html.H2(f"🆕 أحدث ملف مؤرشف: {latest_filename}", style={"textAlign": "center"}),

#     dash_table.DataTable(
#         data=df_latest_pd.to_dict("records"),
#         columns=[{"name": i, "id": i} for i in df_latest_pd.columns],
#         page_size=10,
#         style_table={"overflowX": "auto"},
#         style_cell={"textAlign": "left", "padding": "5px"},
#         style_header={"backgroundColor": "#27ae60", "color": "white", "fontWeight": "bold"}
#     ),

#     html.H2("📂 جميع الملفات المؤرشفة", style={"textAlign": "center", "marginTop": "40px"}),

#     dash_table.DataTable(
#         data=df_archive_pd.to_dict("records"),
#         columns=[{"name": i, "id": i} for i in df_archive_pd.columns],
#         page_size=15,
#         style_table={"overflowX": "auto"},
#         style_cell={"textAlign": "left", "padding": "5px"},
#         style_header={"backgroundColor": "#2c3e50", "color": "white", "fontWeight": "bold"}
#     ),

#     html.H2("📊 تغييرات سجل Delta", style={"textAlign": "center", "marginTop": "40px"}),

#     dcc.Graph(
#         figure=px.histogram(df_log_pd, x="timestamp", color="operation",
#                             title="تسلسل زمني للعمليات", nbins=60)
#     ),

#     dcc.Graph(
#         figure=px.pie(df_log_pd, names="operation", title="نسبة الإضافة مقابل الحذف")
#     ),

#     html.H4("📋 تفاصيل العمليات", style={"textAlign": "center", "marginTop": "30px"}),

#     dash_table.DataTable(
#         data=df_log_pd.to_dict("records"),
#         columns=[{"name": i, "id": i} for i in df_log_pd.columns],
#         page_size=15,
#         style_table={"overflowX": "auto"},
#         style_cell={"textAlign": "left", "padding": "5px"},
#         style_header={"backgroundColor": "#007acc", "color": "white", "fontWeight": "bold"}
#     )
# ])

# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=8050)