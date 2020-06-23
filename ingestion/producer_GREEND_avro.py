from confluent_kafka import Producer
import socket
import time
import json
import csv
import os
import glob
import configparser
import sys
import random
import avro.schema
import avro.io
import io
from typing import Tuple, List, Iterator


def load_config(filepath: str) -> Tuple[str, str, str, str, int]:
    """
    Load config file.

    @param filepath: path to config.ini

    @return datapath: path to data folder

    @return schemapath: path to schema file

    @return brokers: kafka broker addresses

    @return topic: kafka topic to write to

    @return playbackspeed: how many times faster to playback data
    """
    config = configparser.ConfigParser()
    config.read(filepath)

    datapath = config['Data']['greend_path']
    schemapath = config['Data']['schema']
    brokers = config['KafkaBrokers']['address']
    topic = config['KafkaBrokers']['topic_rawdata']
    # how many time faster playback
    playbackspeed = int(config['Data']['playback_speed'])
    return datapath, schemapath, brokers, topic, playbackspeed


def house_reader_GREEND(path: str) -> Tuple[List[str], List[str], List[str]]:
    """
    Load the GREEND file names into lists.

    @param path: the path to the data folder

    @return house_list: list of house names

    @return filenames: file name list for each house

    @return labels: list of appliance names for each house
    """
    house_list = sorted(os.listdir(path))
    house_list = [house for house in house_list if "building" in house]
    filenames = [[] for _ in range(len(house_list))]
    for hid, house in enumerate(house_list):
        hpath = path + '/' + house
        filenames[hid]=sorted([os.path.basename(x) for x in glob.glob(hpath+"/dataset_201*.csv")])

    labels = {}
    with open(path + '/labels.json') as json_file:
        labels = json.load(json_file)
    return house_list, filenames, labels


def open_files(house_name: str, filename: str):
    """
    Open the file for next day

    @rtype fd: file descriptor
    @return fd: next file descriptor

    @rtype csviter: iterator
    @return csviter: csv iterator
    """

    fd = open(datapath + '/' + house_name + '/' + filename, newline='')
    csviter = csv.reader(fd, delimiter=',')
    # skip header
    next(csviter, None)
    return fd, csviter


def fileoffset_init(filenames: List[List[str]], filestart_offset:str) -> List[Iterator[str]]:
    """
    For simulating multiple households
    Shift the starting file in the filename iterator given the file offset.
    """
    filename_iters = [iter(house) for house in filenames]
    for _ in range(int(filestart_offset)):
        for hid, fiter in enumerate(filename_iters):
            filename = next(fiter, None)
            if not filename:
                filename_iters[hid] = iter(filenames[hid])
    return filename_iters


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


def kafka_init(servers: str, topic: str):
    """
    Set up kafka server

    @param servers: all the bootstrap servers with ports, seperated with comma

    @param topic: the name of the kafka topic

    @rtype: object
    @return: kafka producer
    """
    conf = {'bootstrap.servers': servers, 'client.id': socket.gethostname(), 'linger.ms': 50, 'batch.num.messages': 5000}
    producer = Producer(conf)
    return producer


def acked(err, msg):
    """
    For kafka message acknowledgement
    """
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))


def is_number(s: str) -> bool:
    '''
    Check if string is a number
    '''
    try:
        float(s)
        return True
    except ValueError:
        return False


if __name__ == "__main__":
    _, config, filestart_offset, batch_num = sys.argv
    datapath, schemapath, brokers, topic, playbackspeed = load_config(config)
    schema = avro.schema.Parse(open(schemapath).read())
    house_list, filenames, labels = house_reader_GREEND(datapath)
    filename_iters = fileoffset_init(filenames, filestart_offset)
    producer = kafka_init(brokers, topic)

    starttime = time.time()

    # introduce randomness to the power output
    factor = random.uniform(0.8, 1.2)

    # fileds: file descriptors for each house
    # offsets: offset for the current time to the timestamp in the files
    # firsttimes: first timestamp for the current file
    # pretimes: previous timestamp for each house
    # prevalues: previous power values for each house
    # csviters: csv iterator for row reading
    fileds = [None]*len(house_list)
    pretimes = [None]*len(house_list)
    firsttimes = [None]*len(house_list)
    offsets = [None]*len(house_list)
    csviters = [None]*len(house_list)
    prevalues = [[] for _ in range(len(house_list))]

    while True:
        for house_id, house_name in enumerate(house_list):
            # timestamp in millisecond
            current_timestamp = int(round(time.time() * 1000))
            # open a new file if reach eof
            if not pretimes[house_id]:
                # close opened file
                if fileds[house_id]:
                    fileds[house_id].close()
                filename = next(filename_iters[house_id], None)
                # check if the end of the folder files, return to first file
                if not filename:
                    filename_iters[house_id] = iter(filenames[house_id])
                    filename = next(filename_iters[house_id])
                fileds[house_id], csviters[house_id] = open_files(house_name, filename)
                entries = next(csviters[house_id], None)
                firsttimes[house_id] = pretimes[house_id] = float(entries[0])*1000
                prevalues[house_id] = [float(x) if is_number(x) else 0 for x in entries[1:]]
                offsets[house_id] = current_timestamp - float(entries[0])*1000
            # check if the current passes the time inteval of the message
            elif (pretimes[house_id] - firsttimes[house_id]) / playbackspeed + offsets[house_id] + firsttimes[house_id] < current_timestamp:
                for idx, power in enumerate(prevalues[house_id]):
                    house_serial = '2_' + batch_num + '_' + str(house_id)
                    # take care of extra columns
                    label = 'unknown' if idx >= len(labels[house_name]) else labels[house_name][idx]
                    value = {"timestamp": current_timestamp, "house_id": house_serial, "appliance_id": house_serial + '_' + str(idx), "appliance_name": label, "power": power * factor}
                    #print(value)
                    # key: same house in the same kafka partition
                    producer.produce(topic, key=house_serial, value=avro_encoder(schema, value), callback=acked)
                entries = next(csviters[house_id], None)
                if entries:
                    # deal with ocassionally appearing column headers
                    if entries[0] == 'timestamp':
                        entries = next(csviters[house_id])
                    pretimes[house_id] = float(entries[0])*1000
                    prevalues[house_id] = [float(x) if is_number(x) else 0 for x in entries[1:]]
                else:
                    pretimes[house_id] = None
        # asychronous producer
        producer.poll(0)
