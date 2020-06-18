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

def load_config(file):
    """
    Load config file
    """
    config = configparser.ConfigParser()
    config.read(file)

    datapath = config['Data']['greend_path']
    brokers = config['KafkaBrokers']['address']
    topic = config['KafkaBrokers']['topic_rawdata']
    # how many time faster playback
    playbackspeed = int(config['Data']['playback_speed'])
    return datapath, brokers, topic, playbackspeed



def house_reader_GREEND(path):
    '''
    Load the GREEND files into iterators.
    @type path: STRING
    @param path: the path to the data folder

    @rtype house_list: list[STRING]
    @return: list of house names

    @rtype filenames: list[STRING]
    @return: file name list for each house

    @rtype filenames: list[STRING]
    @return: list of appliance names for each house
    '''
    house_list = sorted(os.listdir(path))
    house_list = [house for house in house_list if "building" in house]
    #print(sorted(house_list))
    filenames = [[] for _ in range(len(house_list))]
    for hid, house in enumerate(house_list):
        hpath = path + '/' + house
        filenames[hid]=sorted([os.path.basename(x) for x in glob.glob(hpath+"/dataset_201*.csv")])

    #print(filenames[0])
    # load appliance labels
    labels= {}
    with open(path + '/labels.json') as json_file:
        labels = json.load(json_file)
    return house_list, filenames, labels


def open_files(house_name, filename):
    '''
    Open the file for next day

    @type house_name: STRING
    @param:  house name

    @type filename: STRING
    @param: file name in this house

    @rtype fd: file descriptor
    @return fd: next file descriptor

    @rtype csviter: iterator
    @return csviter: csv iterator
    '''

    fd = open(datapath+ '/' +house_name + '/' + filename, newline='')
    #print(filename)
    csviter = csv.reader(fd, delimiter=',')
    next(csviter, None)
    return fd, csviter


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
    conf = {'bootstrap.servers': servers,'client.id': socket.gethostname(), 'acks':'0'}
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

def is_number(s):
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
    datapath, brokers, topic, playbackspeed = load_config(config)
    starttime=time.time()
    # introduce randomness to the power output
    factor = random.uniform(0.8, 1.2)
    house_list, filenames, labels = house_reader_GREEND(datapath)

    filename_iters = [iter(app) for app in filenames]
    for _ in range(int(filestart_offset)):
        for hid, iter in enumerate(filename_iters):
            filename = next(iter, None)
            if not filename:
                filename_iters[hid] = iter(filenames[hid])

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
    producer = kafka_init(brokers, topic)
    schema_path = "schema.avsc"
    schema = avro.schema.Parse(open(schema_path).read())

    while True:
        for house_id, house_name in enumerate(house_list):
            # timestamp in millisecond
            timestamp = int(round(time.time() * 1000))
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
                    print(filename)
                fileds[house_id], csviters[house_id] = open_files(house_name, filename)
                entries = next(csviters[house_id], None)
                firsttimes[house_id] = pretimes[house_id] = float(entries[0])*1000
                prevalues[house_id] = [float(x) if is_number(x) else 0 for x in entries[1:]]
                offsets[house_id] = timestamp - float(entries[0])*1000
            # check if the current passes the time inteval of the message
            elif (pretimes[house_id]-firsttimes[house_id])/playbackspeed + offsets[house_id] + firsttimes[house_id] < timestamp:
                for idx, power in enumerate(prevalues[house_id]):
                    house_serial = '2_'+batch_num + '_' + str(house_id)
                    label = 'unknown' if idx >= len(labels[house_name]) else labels[house_name][idx]
                    value = {"timestamp": timestamp, "house_id": house_serial, "appliance_id": house_serial + '_' + str(idx), "appliance_name": label, "power": power * factor}
                    #print(value)
                    writer = avro.io.DatumWriter(schema)
                    bytes_writer = io.BytesIO()
                    encoder = avro.io.BinaryEncoder(bytes_writer)
                    writer.write(value, encoder)
                    raw_bytes = bytes_writer.getvalue()
                    # same house in the same partition
                    producer.produce(topic, key=house_serial, value=raw_bytes, callback=acked)
                entries = next(csviters[house_id], None)
                if entries:
                    # deal with ocassionally appeared column headers
                    if entries[0] == 'timestamp':
                        entries = next(csviters[house_id])
                    pretimes[house_id] = float(entries[0])*1000
                    prevalues[house_id] = [float(x) if is_number(x) else 0 for x in entries[1:]]
                else:
                    pretimes[house_id] = None

        producer.flush()
