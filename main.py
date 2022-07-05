from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
import os

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

original = spark.read.csv("trips.csv", header=True) #Read dataframe

df = original.orderBy("origin_coord","destination_coord","datetime") # Trips with similar origin, destination, and time of day should be grouped together.

df = df.withColumn('datetime', df.datetime.cast('timestamp'))
w = F.window("datetime", "7 days")
df2 = df.groupby("region",w).agg(F.count("region").alias("counted_region")) #create a window function and count the number of trips per 7 days by region
df2 = df2.withColumn('counted_region_avg', df2.counted_region/7) # divide the count by 7 to have the avg per rergion per week
print("Printing the avg trips per week in a region gruped by 7 days")
df2.select("region","window","counted_region_avg" ).show()

original.write.format("jdbc").option("url", f"jdbc:mysql://{os.getenv('DATABASE_ENDPOINT')}/{os.getenv('DATABASE_NAME')}&useUnicode=true&characterEncoding=UTF-8&useSSL=false") \
	.option("driver", "com.mysql.jdbc.Driver").option("dbtable", {os.getenv('TABLE_NAME')}) \
	.option("user", os.getenv("DATABASE_USER")).option("password",os.getenv("DATABASE_PASSWORD")).save()
