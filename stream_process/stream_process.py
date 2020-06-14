from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json,col
from pyspark.sql.functions import from_utc_timestamp
from pyspark.sql.functions import window, collect_list,struct, to_json, pandas_udf, PandasUDFType
import pyspark.sql.window
import pandas as pd


milli=1000

schema_g = StructType([ \
    StructField("time_window", ArrayType(StructType([StructField("time_window_start", TimestampType()), \
                StructField("time_window_start", TimestampType())]))), \
    StructField("house_id", IntegerType()), \
    StructField("appliance_id", IntegerType()), \
    StructField("values", ArrayType(StructType([StructField("timestamp", LongType()), \
                StructField("power", FloatType())]))) \
])

schema_g = StructType([ \
    StructField("ts",ArrayType(StructType([StructField("timestamp", LongType()), StructField("power", FloatType())])))
])


@pandas_udf(schema_g, PandasUDFType.GROUPED_MAP)
def his_analy(df):
    '''
    Check if the appliance is regularly used during this time (the time window: "win" in milliseconds) of the day in the past "n" days
    Note that the data is fast-playbacked by "v" times
    '''

    n = 3
    v = 24
    win = 30*1000
    result_pdf = df.select("*").toPandas()
    print(result_pdf)
    return df.select()


spark = SparkSession.builder.appName("test").getOrCreate()
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "b-2.kafka.3336h5.c4.kafka.us-east-1.amazonaws.com:9092,b-1.kafka.3336h5.c4.kafka.us-east-1.amazonaws.com:9092")\
  .option("subscribe", "powerraw") \
  .option("startingOffsets", "earliest") \
  .load()
df=df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

schema = StructType([StructField("timestamp", LongType()), \
        StructField("house_id", IntegerType()), \
        StructField("appliance_id", IntegerType()), \
        StructField("appliance_name", StringType()), \
        StructField("power", FloatType())])

df=df.withColumn("value", from_json(df.value, schema))\
  .select((col("value.timestamp")/milli).alias("time").cast(TimestampType()),\
  col("value.house_id"),col("value.appliance_id"),struct(["value.timestamp", "value.power"]).alias("values")) #value.* for seperate columns

#df1=df.withWatermark("time", "2 seconds").groupBy(window(col("time"),"5 seconds","3 second"),"house_id","appliance_id").agg(collect_list("values"))
df2=df.withWatermark("time", "2 seconds").groupBy(window(col("time"),"11 seconds","11 second"),"house_id","appliance_id").apply(his_analy)

#query=df1.writeStream.outputMode("append").format("console").option("truncate", False).start()
query2=df2.writeStream.outputMode("append").format("console").option("truncate", False).start()
'''
query = df1.withColumn("value", to_json(struct("house_id","appliance_id","collect_list(values)"))) \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "b-1.kafka.3336h5.c4.kafka.us-east-1.amazonaws.com:9092,b-2.kafka.3336h5.c4.kafka.us-east-1.amazonaws.com:9092") \
  .option("topic", "powerf") \
  .option("checkpointLocation", "checkpoints") \
  .start()

query2 = df2.withColumn("value", to_json(struct("house_id","appliance_id","collect_list(values)"))) \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "b-1.kafka.3336h5.c4.kafka.us-east-1.amazonaws.com:9092,b-2.kafka.3336h5.c4.kafka.us-east-1.amazonaws.com:9092") \
  .option("topic", "powers") \
  .option("checkpointLocation", "checkpoints2") \
  .start()
'''
#query.awaitTermination()
query2.awaitTermination()
