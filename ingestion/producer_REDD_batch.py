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
import copy

def load_config(file):
    """
    Load config file
    """
    config = configparser.ConfigParser()
    config.read(file)

    datapath = config['Data']['redd_path']
    brokers = config['KafkaBrokers']['address']
    topic = config['KafkaBrokers']['topic_rawdata']
    # how many time faster playback
    playbackspeed = int(config['Data']['playback_speed'])
    return datapath, brokers, topic, playbackspeed

def house_reader_REDD(datapath, filestart_offset):
    '''
    Load the REDD files into file descriptors.
    @type datapath: STRING
    @param datapath: the path to the data folder

    @type filestart_offset: int
    @param filestart_offset: shift the starting point by ths number of entries

    @rtype fileds: list[list[file_descriptor]]
    @return: files descriptors for each appliances in each house

    @rtype fileds: list[list[file_iterator]]
    @return: files iterator for each appliances in each house

    @rtype labels: list[list[STRING]]
    @return: labels for each appliances in each house

    @rtype offsets: list[list[long]]
    @return: time offests between first timestamp and the execution time

    @rtype powers: list[list[float]]
    @return: first power values
    '''
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
                entries = next(fileiter[hid][-1],None)
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
            #print(labels[hid])
            labels[hid].append(row[1])
    return fileds, fileiter, labels, offsets, values


def close_files(fileds):
    '''
    close all file descriptors
    @type fileds: list[list[file_descriptor]]
    @param: files descriptors for each appliances in each house
    '''
    for house in fileds:
        for f in house:
            f.close()

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
    _, config, filestart_offset, batch_num = sys.argv
    datapath, brokers, topic, playbackspeed = load_config(config)
    # introduce randomness to the power output
    factor = random.uniform(0.8, 1.2)
    producer = kafka_init(brokers, topic)
    fileds, fileiter, labels, offsets, values = house_reader_REDD(datapath, int(filestart_offset))
    first_fileiter = copy.deepcopy(fileiter)
    # Get the first timestamps for each appliances
    first_values = copy.deepcopy(values)

    while True:
        try:
            for house_id, appliances in enumerate(fileiter):
                for app_id, row in enumerate(appliances):
                    # timestamp in millisecond
                    timestamp = int(round(time.time() * 1000))
                    # check if the end of file and if the current time elaspe is larger than the next data point
                    if values[house_id][app_id]:
                        if (values[house_id][app_id][0]-first_values[house_id][app_id][0])/playbackspeed + offsets[house_id][app_id] + first_values[house_id][app_id][0] < timestamp:
                            house_serial = '1_'+batch_num + '_' + str(house_id)
                            value = {"timestamp": timestamp, "house_id": house_serial, "appliance_id": house_serial + '_' + str(app_id), "appliance_name": labels[house_id][app_id], "power": values[house_id][app_id][1] * factor}
                            print(value)
                            producer.produce(topic, key='key', value=json.dumps(value), callback=acked)
                            # read next row
                            entries = next(row, None)
                            values[house_id][app_id] = (int(entries[0])*1000, float(entries[1])) if entries else None
                    else:
                        fileds[house_id][app_id].seek(0)
                        entries = next(fileiter[house_id][app_id])
                        offsets[house_id][app_id] = int(round(time.time() * 1000)) - int(entries[0])*1000
                        first_values = (int(entries[0])*1000, float(entries[1]))
            producer.flush()
        except KeyboardInterrupt:
            close_files(fileds)
            break
    close_files(fileds)
