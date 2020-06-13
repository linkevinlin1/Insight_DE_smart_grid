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

kafka_servers = 'b-1.kafka.0ofh61.c4.kafka.us-east-1.amazonaws.com:9092,b-2.kafka.0ofh61.c4.kafka.us-east-1.amazonaws.com:9092'
topic = 'history'
druid_server = 'http://ec2-34-206-118-121.compute-1.amazonaws.com:8082'
#pd.set_option('display.max_columns', 500)

playback = 24 # n times
window = 20  # in minutes
convert_win = int(window * 60 / 24)
lookbackdays = 5
# output timestamp is at the middle of the window for next day


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


query = PyDruid(druid_server, 'druid/v2')

producer = kafka_init(kafka_servers, topic)

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
    datasource='powerraw1_2',
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
