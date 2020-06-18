from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json,col
from pyspark.sql.functions import window, to_json, when, count, struct
from pyspark.sql.column import Column, _to_java_column
from pyspark import SparkContext

milli=1000


def from_avro(col, jsonFormatSchema):
    sc = SparkContext._active_spark_context
    avro = sc._jvm.org.apache.spark.sql.avro
    f = getattr(getattr(avro, "package$"), "MODULE$").from_avro
    return Column(f(_to_java_column(col), jsonFormatSchema))


avro_schema = """
 {
   "namespace": "powerraw",
   "name": "valueall",
   "type": "record",
   "fields" : [
     {
       "name" : "house_id",
       "type" : "string"
     },
     {
       "name" : "appliance_name",
       "type" : "string"
     },
     {
       "name" : "appliance_id",
       "type" : "string"
     },
     {
       "name" : "timestamp",
       "type" : "long"
     },
     {
       "name" : "power",
       "type" : "float"
     }
   ]
}
"""


kafka_servers = 'b-3.kafka.hasav4.c4.kafka.us-east-1.amazonaws.com:9092,b-1.kafka.hasav4.c4.kafka.us-east-1.amazonaws.com:9092,b-2.kafka.hasav4.c4.kafka.us-east-1.amazonaws.com:9092'
in_topic = 'powerraw'
out_topic = 'dutycycle'
playbackspeed = 24
window_len = 10
powerthres = 5.0
watermark = 2

window_converted = window_len * 60 // playbackspeed


spark = SparkSession.builder.appName("dutycycle").getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_servers)\
  .option("subscribe", in_topic) \
  .option("failOnDataLoss", "false") \
  .load()

df=df.withColumn("value", from_avro("value", avro_schema))\
  .select((col("value.timestamp")/milli).alias("time").cast(TimestampType()),col("value.house_id"),col("value.appliance_id"),col("value.appliance_name"),col("value.power")) #value.* for seperate columns

out=df.withWatermark("time", str(watermark) + " seconds").groupBy(window(col("time"), str(window_converted) + " seconds", str(window_converted//5) + " second"),"house_id","appliance_id") \
  .agg(count("power").alias("c_all"),count(when(col("power") > powerthres, True)).alias("c_duty")) \
  .withColumn("duty_cycle", (col("c_duty")/col("c_all"))).withColumn("time_end", col("window.end")) \
  .drop("window", "c_all", "c_duty")

#query=out.writeStream.outputMode("append").format("console").option("truncate", False).start()

query = out.withColumn("value", to_json(struct("time_end","house_id","appliance_id","duty_cycle"))) \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_servers) \
  .option("topic", out_topic) \
  .option("checkpointLocation", "checkpoints") \
  .start()

query.awaitTermination()
