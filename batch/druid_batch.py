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

def load_config(file):
    """
    Load config file
    """
    config = configparser.ConfigParser()
    config.read(file)

    druid_address = config['Druid']['address']
    lookbackdays = config['Druid']['lookbackdays']
    window = config['Druid']['window']
    brokers = config['KafkaBrokers']['address']
    out_topic = config['KafkaBrokers']['topic_batch']
    # how many time faster playback
    playbackspeed = int(config['Data']['playback_speed'])

    return druid_address, lookbackdays, window, brokers, out_topic, playbackspeed


def kafka_init(servers,topic):
    '''
    Set up kafka server

    @type servers: STRING
    @param servers: all the bootstrap servers with ports, seperated with comma

    @type topic: STRING
    @param topic: the name of the kafka topic

    @rtype: object
    @return: kafka producer
    '''
    conf = {'bootstrap.servers': servers,'client.id': socket.gethostname()}
    producer = Producer(conf)
    return producer

def acked(err, msg):
    '''
    Used for kafka message acknowledgement
    '''
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    #else:
        #print("Message produced: %s" % (str(msg)))


if __name__ == "__main__":
    _, config = sys.argv
    druid_server, lookbackdays, window, brokers, out_topic, playback = load_config(config)
    convert_win = int(window * 60 / 24)

    query = PyDruid(druid_server, 'druid/v2')

    producer = kafka_init(kafka_servers, out_topic)

    starttime=time.time()

    then = datetime.now()
    utc_timestamp = int(time.mktime(then.timetuple())*1e3 + then.microsecond/1e3 - convert_win * 1000 / 2 + 86400 * 1000 / playback)
    print(utc_timestamp)
    interval_list = [''] * lookbackdays
    now = datetime.utcnow() - timedelta(seconds = convert_win + (60//playback) )

    for i in range(lookbackdays):
        now = now - timedelta(seconds = 86400 / playback)
        interval_list[i] = now.replace(microsecond=0).isoformat() + '/pt' + str(convert_win*2) + 's'
    interval_list.reverse()
    print(interval_list)




    group = query.groupby(
        datasource = in_topic,
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

    #print(df)

    time_json= {"timestamp": utc_timestamp}

    for i, row in df.iterrows():
        value_out = row.to_json()[:-1]+','+json.dumps(time_json)[1:]
        producer.produce(topic, key='key', value = value_out, callback=acked)
        print(value_out)
        if i % 50 == 0:
             producer.flush()
