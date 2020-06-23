from pydruid import *
from pydruid.client import *
from pydruid.query import QueryBuilder
from pydruid.utils.postaggregator import *
from pydruid.utils.aggregators import *
from pydruid.utils.filters import *
import pandas as pd
from confluent_kafka import Producer
import socket
import time
import json
from datetime import datetime, timedelta
import configparser
import sys
from typing import Tuple


def load_config(filepath: str) -> Tuple[str, int, str, str, str, int]:
    """
    Load config file

    @param filepath: path to config.ini

    @return datapath: path to data folder

    @return lookbackdays: how many days to look back for historical usage

    @return window: the window to average for each day in minutes

    @return brokers: kafka broker addresses

    @return in_topic: datasource name in druid

    @return out_topic: kafka topic to write to

    @return playbackspeed: how many times faster to playback data
    """
    config = configparser.ConfigParser()
    config.read(pathfile)

    druid_address = config['Batch']['address']
    lookbackdays = int(config['Batch']['lookbackdays'])
    window = int(config['Batch']['window'])
    brokers = config['KafkaBrokers']['address']
    out_topic = config['KafkaBrokers']['topic_batch']
    in_topic = config['KafkaBrokers']['topic_rawdata']
    # how many time faster playback
    playbackspeed = int(config['Data']['playback_speed'])

    return druid_address, lookbackdays, window, brokers, in_topic, out_topic, playbackspeed


def kafka_init(servers: str, topic: str):
    """
    Set up kafka server

    @param servers: all the bootstrap servers with ports, seperated with comma

    @param topic: the name of the kafka topic

    @rtype: object
    @return: kafka producer
    """
    conf = {'bootstrap.servers': servers, 'client.id': socket.gethostname()}
    producer = Producer(conf)
    return producer


def acked(err, msg):
    """
    Used for kafka message acknowledgement
    """
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))


if __name__ == "__main__":
    _, config = sys.argv
    druid_server, lookbackdays, window, kafka_servers, in_topic, out_topic, playback = load_config(config)
    convert_win = int(window * 60 / playback)
    query = PyDruid(druid_server, 'druid/v2')
    producer = kafka_init(kafka_servers, out_topic)

    then = datetime.now()
    utc_timestamp = int(time.mktime(then.timetuple())*1e3 + then.microsecond/1e3)
    interval_list = [''] * lookbackdays
    now = datetime.utcnow() - timedelta(seconds = convert_win + (60//playback) ) # add little tolerance

    # create time intervals for the past 5 days at this time of the day
    for i in range(lookbackdays):
        now = now - timedelta(seconds = 86400 / playback)
        interval_list[i] = now.replace(microsecond=0).isoformat() + '/pt' + str(convert_win*2) + 's'
    interval_list.reverse()

    # Druid query
    group = query.groupby(
        datasource=in_topic,
        granularity='all',
        intervals=interval_list,
        dimensions=["house_id", "appliance_id"],
        aggregations={"count_w": longsum("count"), "sum_power_w": doublesum("sum_power")},
        post_aggregations={'avg_power': (Field('sum_power_w') / Field('count_w'))}
    )

    df = query.export_pandas()
    del df["count_w"]
    del df["sum_power_w"]
    del df["timestamp"]

    time_json= {"timestamp": utc_timestamp}

    for i, row in df.iterrows():
        value_out = row.to_json()[:-1]+','+json.dumps(time_json)[1:]
        producer.produce(out_topic, key='key', value = value_out, callback=acked)
        if i % 50 == 0:
             producer.flush()
