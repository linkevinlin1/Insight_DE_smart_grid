from confluent_kafka import Producer
import socket
import time
import csv
import os
import glob
import configparser
import sys
import random
import copy
import avro.schema
import avro.io
import io
from typing import Tuple


def load_config(filepath: str) -> Tuple[str, str, str, str, int]:
    """
    Load config file

    @param filepath: path to config.ini

    @return datapath: path to data folder

    @return schemapath: path to schema file

    @return brokers: kafka broker addresses

    @return topic: kafka topic to write to

    @return playbackspeed: how many times faster to playback data
    """
    config = configparser.ConfigParser()
    config.read(filepath)

    datapath = config['Data']['redd_path']
    schemapath = config['Data']['schema']
    brokers = config['KafkaBrokers']['address']
    topic = config['KafkaBrokers']['topic_rawdata']
    # how many time faster playback
    playbackspeed = int(config['Data']['playback_speed'])
    return datapath, schemapath, brokers, topic, playbackspeed

def house_reader_REDD(datapath: str, filestart_offset: int):
    """
    Load the REDD files into file descriptors.

    @param datapath: the path to the data folder
    @param filestart_offset: shift the starting point by this number of days

    @rtype fileds: list[list[file_descriptor]]
    @return: files descriptors for each appliances in each house

    @rtype fileiter: list[list[file_iterator]]
    @return: files iterator for each appliances in each house

    @rtype labels: list[list[STRING]]
    @return: labels for each appliances in each house

    @rtype offsets: list[list[long]]
    @return: time offests between first timestamp and the execution time

    @rtype values: list[list[Tuple[int,float]]
    @return: first (timestamp, power)
    """
    house_list = sorted(os.listdir(datapath))
    fileds = [[] for _ in range(len(house_list))]
    fileiter = [[] for _ in range(len(house_list))]
    labels =[[] for _ in range(len(house_list))]
    offsets = [[] for _ in range(len(house_list))]
    values = [[] for _ in range(len(house_list))]

    for hid, house in enumerate(house_list):
        hpath = datapath + '/' + house
        for filename in glob.glob(hpath+"/channel_*.dat"):
            f = open(filename, newline='')
            fileds[hid].append(f)
            fileiter[hid].append(csv.reader(f, delimiter=' '))
            entries = next(fileiter[hid][-1])
            # shift the staring entry
            for _ in range(filestart_offset*86400//3):
                entries = next(fileiter[hid][-1], None)
                # read from the beginning if reaches the eof
                if not entries:
                    f.seek(0)
                    entries = next(fileiter[hid][-1],None)

            # time offest in millisecond
            offsets[hid].append(int(round(time.time() * 1000)) - int(entries[0])*1000)
            # (timestamp, power)
            values[hid].append( (int(entries[0])*1000, float(entries[1])) )
        # read labels for the appliances
        lpath = datapath + '/' + house + '/labels.dat'
        labelf = open(lpath, newline='')
        lreader = csv.reader(labelf, delimiter=' ')
        for row in lreader:
            labels[hid].append(row[1])
    return fileds, fileiter, labels, offsets, values


def close_files(fileds):
    """
    close all file descriptors

    @type fileds: list[list[file_descriptor]]
    @param: files descriptors for each appliances in each house
    """
    for house in fileds:
        for f in house:
            f.close()


def kafka_init(servers: str, topic: str):
    """
    Set up kafka server

    @param servers: all the bootstrap servers with ports, seperated with comma

    @param topic: the name of the kafka topic

    @rtype: object
    @return: kafka producer
    """
    conf = {'bootstrap.servers': servers, 'client.id': socket.gethostname(), 'linger.ms': 500, 'batch.num.messages': 5000}
    producer = Producer(conf)
    return producer


def acked(err, msg):
    """
    Used for kafka message acknowledgement
    """
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))


def avro_encoder(schema, value: dict):
    """
    Encode dictionary to avro format with designated schema
    """
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(value, encoder)
    raw_bytes = bytes_writer.getvalue()
    return raw_bytes



if __name__ == "__main__":
    _, config, filestart_offset, batch_num = sys.argv
    datapath, schemapath, brokers, topic, playbackspeed = load_config(config)
    schema = avro.schema.Parse(open(schemapath).read())
    fileds, fileiter, labels, offsets, values = house_reader_REDD(datapath, int(filestart_offset))
    producer = kafka_init(brokers, topic)
    # Get the first timestamps for each appliances
    first_values = copy.deepcopy(values)
    # introduce randomness to the power output
    factor = random.uniform(0.8, 1.2)

    while True:
        try:
            for house_id, appliances in enumerate(fileiter):
                for app_id, row in enumerate(appliances):
                    # timestamp in millisecond
                    timestamp = int(round(time.time() * 1000))
                    if values[house_id][app_id]:
                        # check if the end of file and if the current time elaspe is larger than the next data point
                        if (values[house_id][app_id][0]-first_values[house_id][app_id][0])/playbackspeed + offsets[house_id][app_id] + first_values[house_id][app_id][0] < timestamp:
                            house_serial = '1_'+batch_num + '_' + str(house_id)
                            value = {"timestamp": timestamp, "house_id": house_serial, "appliance_id": house_serial + '_' + str(app_id), "appliance_name": labels[house_id][app_id], "power": values[house_id][app_id][1] * factor}
                            # same house in the same partition
                            producer.produce(topic, key=house_serial, value=avro_encoder(schema, value), callback=acked)
                            # read next row
                            entries = next(row, None)
                            values[house_id][app_id] = (int(entries[0])*1000, float(entries[1])) if entries else None
                    # when reach eof
                    else:
                        fileds[house_id][app_id].seek(0)
                        entries = next(fileiter[house_id][app_id])
                        offsets[house_id][app_id] = int(round(time.time() * 1000)) - int(entries[0])*1000
                        first_values[house_id][app_id] = values[house_id][app_id] = (int(entries[0])*1000, float(entries[1]))
            # asychronous producer
            producer.poll(0)
        except KeyboardInterrupt:
            close_files(fileds)
            break
    close_files(fileds)
