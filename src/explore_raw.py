from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ed-pipeline-explore")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("data/raw/ed_attendances_raw.csv", header=True, inferSchema=False)

print("Rows:", df.count())
print("Distinct attendance_ids:", df.select("attendance_id").distinct().count())
df.printSchema()
df.show(5, truncate=False)

spark.stop()