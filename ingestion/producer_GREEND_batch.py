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
# period for the data points
playbackspeed = 24

labels = {
    'building0': ['coffee machine', 'washing machine', 'radio', 'water kettle', 'fride w/ freezer', 'dishwasher', 'ktichen lamp', 'TV', 'vacuum cleaner'],
    'building1': ['radio', 'freezer', 'dishwasher', 'fridge', 'washing machine', 'water kettle', 'blender', 'network router','unknown'],
    'building2': ['fridge', 'dishwasher', 'microwave', 'water kettle', 'washing machine', 'radio w/ amplifier', 'dryier', 'kitchenware (mixer and fruit juicer)', 'bedside light'],
    'building3': ['TV', 'NAS', 'washing machine', 'drier', 'dishwasher', 'notebook', 'kitchenware', 'coffee machine', 'bread machine'],
    'building4': ['entrance outlet', 'dishwasher', 'water kettle', 'fridge w/o freezer', 'washing machine', 'hairdrier', 'computer', 'coffee machine', 'TV'],
    'building5': ['total outlets', 'total lights', 'kitchen TV', 'living room TV', 'fridge w/ freezer', 'electric oven', 'computer w/ scanner and printer', 'washing machine', 'hood', 'unknown'],
    'building6': ['plasma TV', 'lamp', 'toaster', 'hob', 'iron', 'computer w/ scanner and printer', 'LCD TV', 'washing machine', 'fridge w/ freezer'],
    'building7': ['hair dryer', 'washing machine', 'videogame console and radio', 'dryer', 'TV w/ decoder and computer in living room', 'kitchen TV', 'dishwasher', 'total outlets', 'total lights'],
    'building8': ['kitchen TV', 'dishwasher', 'living room TV', 'desktop computer w/ screen', 'washing machine', 'bedroom TV', 'total outlets', 'total lights'],
    }

def house_reader_GREEND(datapath):
    '''
    Load the GREEND files into iterators.
    @type datapath: STRING
    @param datapath: the path to the data folder

    @rtype house_list: list[STRING]
    @return: list of house names

    @rtype filenames: list[iterator]
    @return: file name iterator for each house
    '''
    path = datapath+'/GREEND'
    house_list = sorted(os.listdir(path))
    house_list = [house for house in house_list if "building" in house]
    #print(sorted(house_list))
    filenames = [[] for _ in range(len(house_list))]
    for hid, house in enumerate(house_list):
        hpath = path + '/' + house
        filenames[hid]=iter(sorted([os.path.basename(x) for x in glob.glob(hpath+"/dataset_201*.csv")]))
    #print(filenames[0])
    return house_list, filenames


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

    fd = open(datapath+'/GREEND/'+house_name + '/' + filename, newline='')
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

def is_number(s):
    '''
    Check if string is a number
    '''
    try:
        float(s)
        return True
    except ValueError:
        return False

i=0
starttime=time.time()
house_list, filenames = house_reader_GREEND(datapath)

first_filenames = filenames
# file descriptors for each house
fileds = [None]*len(house_list)
# offset for the current time to the timestamp in the files
offsets = [None]*len(house_list)
# first timestamp for the current file
firsttimes = [None]*len(house_list)
# previous timestamp for each house
pretimes = [None]*len(house_list)
# previous power values for each house
prevalues = [None]*len(house_list)
# csv iterator for row reading
csviters = [None]*len(house_list)

producer = kafka_init(servers, topic)

while True:
    print(i)
    for house_id, house_name in enumerate(house_list):
        # timestamp in millisecond
        timestamp = int(round(time.time() * 1000))
        # open a new file if reach eof
        if not pretimes[house_id]:
            # close opened file
            if fileds[house_id]:
                fileds[house_id].close()
            filename = next(filenames[house_id], None)
            # check if the end of the folder files, return to first file
            if not filename:
                filenames[house_id] = first_filenames[house_id]
                filename = next(filenames[house_id])
            fileds[house_id], csviters[house_id] = open_files(house_name, filename)
            entries = next(csviters[house_id], None)
            firsttimes[house_id] = pretimes[house_id] = float(entries[0])*1000
            prevalues[house_id] = [float(x) if x != 'NULL' else 0 for x in entries[1:]]
            offsets[house_id] = timestamp - float(entries[0])*1000
        # check if the current passes the time inteval of the message
        elif (pretimes[house_id]-firsttimes[house_id])/playbackspeed + offsets[house_id] + firsttimes[house_id] < timestamp:
            for idx, power in enumerate(prevalues[house_id]):
                value = {"timestamp": timestamp, "house_id": 2000+house_id, "appliance_id": idx, "appliance_name": labels[house_name][idx], "power": power}
                print(value)
                producer.produce(topic, key='key', value=json.dumps(value), callback=acked)
            entries = next(csviters[house_id], None)
            if entries:
                # deal with ocassionally appeared column headers
                if entries[0] == 'timestamp':
                    entries = next(csviters[house_id])
                pretimes[house_id] = float(entries[0])*1000
                prevalues[house_id] = [float(x) if is_number(x) else 0 for x in entries[1:]]
            else:
                pretimes[house_id] = prevalues[house_id] = None

    producer.flush()

    i+=1
