# consumer_main.py                                                                                                               
# This is a user facing message_service utility that receives messages from a message_producer
# at the host port and topic specified by that producer.
# The default hostname port  and topic is localhost, 61613 and /topic/TracksDataObject. 
# This utility can subscribe to multiple topics simultaneously by separating the topic string by commas

from message_consumer import message_consumer
import time
import queue
import getopt
import sys

def main():
    hostname=None
    port=None
    topic=None
    maxMessages=None
    verbose=False
    subscriberID=None

    # Input Arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:], "ho:p:t:m:s:v", ["help", "host=", "port=", "topic=", "maxMessages=", "subscriberID=", "verbose="])
    except getopt.GetoptError as err:
        print (str(err))
        sys.exit(2)
    for opt, arg in opts:
        if opt in ['-o', '--host']:
            hostname = arg
        elif opt in ['-p', '--port']:
            port = arg
        elif opt in ['-t', '--topic']:
            topic =  arg.split(',') 
        elif opt in ['-m', '--maxMessages']:
            maxMessages = arg
        elif opt in ['-s', '--subscriberID']:
            subscriberID = arg
        elif opt in ['-v', '--verbose']:
            verbose = True

    # Default host, port and topic
    if (hostname is None):
        hostname = 'localhost'
    if (port is None):
        port = 61613
    if (topic is None):
        topic = ['TracksDataObject']
    if (subscriberID is None):
        subscriberID = 1
    
    topicFull = []
    for t in topic:
        topicFull.append('/topic/' + t) #'/topic/'+t)
        
    # Queue messages
    msg_queue = queue.Queue()
    if (maxMessages is None):
        c = message_consumer(topicFull, subscriberID, msg_queue, verbose)
    else:
        c = message_consumer(topicFull, subscriberID, msg_queue, verbose, maxMessages)
    c.start(hostname, port)
    c.subscribe()

    try: 
        while True:
            #time.sleep(1)
            if(not c.msg_queue.empty()):
                print(c.msg_queue.get())
    except KeyboardInterrupt:
        print('Process terminated by Ctrl+C. Exiting.')
        c.stop();
        pass

    c.stop();

main()
