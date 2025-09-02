from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("wordcount")
    .getOrCreate()
)

text = spark.read.text("/opt/spark/apps/wordcount.py")
counts = (
    text.selectExpr("explode(split(value, '\\\\W+')) as word")
        .where("word <> ''")
        .groupBy("word").count()
        .orderBy("count", ascending=False)
)

counts.show(20, truncate=False)

spark.stop()
