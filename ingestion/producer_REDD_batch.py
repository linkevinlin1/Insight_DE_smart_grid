from confluent_kafka import Producer
import socket
import time
import json
import csv
import os
import glob

datapath = '../../data'
servers = 'localhost:9092'
topic = "powerraw"
# how many time faster playback
playbackspeed = 24


def house_reader_REDD(datapath):
    '''
    Load the REDD files into file descriptors.
    @type datapath: STRING
    @param datapath: the path to the data folder

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
    path_REDD = datapath+'/REDD'
    house_list=sorted(os.listdir(path_REDD))
    fileds = [[] for _ in range(len(house_list))]
    fileiter = [[] for _ in range(len(house_list))]
    labels = [[] for _ in range(len(house_list))]
    offsets = [[] for _ in range(len(house_list))]
    values = [[] for _ in range(len(house_list))]
    for hid, house in enumerate(house_list):
        hpath = path_REDD + '/' + house
        for filename in glob.glob(hpath+"/channel_*.dat"):
            f = open(filename, newline='')
            fileds[hid].append(f)
            fileiter[hid].append(csv.reader(f, delimiter=' '))
            entries = next(fileiter[hid][-1])
            # time offest in millisecond
            offsets[hid].append(int(round(time.time() * 1000)) - int(entries[0])*1000)
            # (timestamp, power)
            values[hid].append( (int(entries[0])*1000, float(entries[1])) )
        # read labels for the appliances
        lpath = path_REDD + '/' + house + '/labels.dat'
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


i=0
starttime=time.time()
producer = kafka_init(servers, topic)
fileds, fileiter, labels, offsets, values = house_reader_REDD(datapath)

# Get the first timestamps for each appliances
first_values = values
#print(offsets)
while True:
    print(i)
    try:
        for house_id, appliances in enumerate(fileiter):
            for app_id, row in enumerate(appliances):
                # timestamp in millisecond
                timestamp = int(round(time.time() * 1000))
                # check if the end of file and if the current time elaspe is larger than the next data point
                if values[house_id][app_id]:
                    if (values[house_id][app_id][0]-first_values[house_id][app_id][0])/playbackspeed + offsets[house_id][app_id] + first_values[house_id][app_id][0] < timestamp:
                        value = {"timestamp": timestamp, "house_id": 1000+house_id, "appliance_id": app_id, "appliance_name": labels[house_id][app_id], "power": values[house_id][app_id][1]}
                        producer.produce(topic, key='key', value=json.dumps(value), callback=acked)
                        # read next row
                        entries = next(row, None)
                        values[house_id][app_id] = (int(entries[0])*1000, float(entries[1])) if entries else None
                else:
                    fileds, fileiter, labels, offsets, values = house_reader_REDD(datapath)
                    first_values = values
                    # TO DO do this to individual files
        producer.flush()
    except KeyboardInterrupt:
        close_files(fileds)
        break
    i += 1

close_files(fileds)
