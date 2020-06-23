from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import window, to_json, when, count, struct, col
from pyspark.sql.column import Column, _to_java_column
from pyspark import SparkContext
from typing import Tuple
import configparser

milli = 1000


def from_avro(col, jsonFormatSchema):
    """
    Convert Avro to JSON
    """
    sc = SparkContext._active_spark_context
    avro = sc._jvm.org.apache.spark.sql.avro
    f = getattr(getattr(avro, "package$"), "MODULE$").from_avro
    return Column(f(_to_java_column(col), jsonFormatSchema))


def load_config(filepath: str) -> Tuple[str, int, float, str, str, str, int]:
    """
    Load config file.

    @param filepath: path to config.ini

    @return window: time window in minutes

    @return watermark: watermark window in seconds

    @return powerthres: power threshold for active usage

    @return brokers: kafka broker addresses

    @return in_topic: kafka topic to read from

    @return out_topic: kafka topic to write to

    @return playbackspeed: how many times faster to playback data
    """
    config = configparser.ConfigParser()
    config.read(filepath)

    window = int(config['Spark']['window'])
    watermark = config['Spark']['watermark']
    powerthres = float(config['Spark']['powerthres'])
    brokers = config['KafkaBrokers']['address']
    in_topic = config['KafkaBrokers']['topic_rawdata']
    out_topic = config['KafkaBrokers']['topic_stream']
    # how many time faster playback
    playbackspeed = int(config['Data']['playback_speed'])
    return window, watermark, powerthres, brokers, in_topic, out_topic, playbackspeed


# read schema
jsonFormatSchema = open("schema.avsc", "r").read()
window_len, watermark, powerthres, kafka_servers, in_topic, out_topic, playbackspeed = load_config("config.ini")
window_converted = window_len * 60 // playbackspeed

spark = SparkSession.builder.appName("dutycycle").getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_servers) \
  .option("subscribe", in_topic) \
  .option("failOnDataLoss", "false") \
  .load()

df=df.withColumn("value", from_avro("value", jsonFormatSchema)) \
  .select((col("value.timestamp")/milli).alias("time") \
  .cast(TimestampType()),col("value.house_id"),col("value.appliance_id"),col("value.appliance_name"),col("value.power"))

out=df.withWatermark("time", watermark + " seconds") \
  .groupBy(window(col("time"), str(window_converted) + " seconds", str(window_converted//5) + " second"),"house_id","appliance_id") \
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
