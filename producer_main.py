# producer_main.py
# This is a user facing message_service utility that sends the text content of a file [fileIn]
# to a consumer at the host port and topic specified here.
# The default hostname port  and topic is localhost, 61613 and /topic/TracksDataObject

from message_producer import message_producer
import time
import getopt
import sys
from os import path
import os
sys.path.append(os.environ.get('HOME') + '/local/src/neptunepg/src')
from read_json_msg import read_json_msg

def main():
    hostname=None
    port=None
    topic=None
    multiline=None
    verbose=False
    fileIn=None
    
    # Input Arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:], "ho:p:t:m:f:v", ["help", "host=", "port=", "topic=", "multilineMessages=", "file=", "verbose="])
    except getopt.GetoptError as err:
        print (str(err))
        sys.exit(2)
    for opt, arg in opts:
        if opt in ['-o', '--host']:
            hostname = arg
        elif opt in ['-p', '--port']:
            port = arg
        elif opt in ['-t', '--topic']:
            topic = arg
        elif opt in ['-m', '--multilineMessages']:
            multiline = arg
        elif opt in ['-f', '--file']:
            fileIn = arg 
        elif opt in ['-v', '--verbose']:
            verbose = True
    
    # Default host, port and topic
    if (hostname is None):
        hostname = 'localhost'
    if (port is None):
        port = 61613
    if (topic is None):
        topic = 'TracksDataObject'
    assert (not (fileIn is None)), 'User must specify a file to send! (-f) <path/filename>'
    assert (path.isfile(fileIn)), 'Specified filename is not valid!'

    topic = '/topic/' + topic

    if (verbose):
        print("hostname ", hostname)
        print("port ", port)
        print("topic ", topic)

    # Call message_producer constructor
    p = message_producer(hostname, port, topic, verbose)
    
    # Open Input File
    print(fileIn)
    fo = open(fileIn, "r").readlines()
    
    print('Sending json messages.')
    while fo:
        msg, fo = read_json_msg(fo, delay = 0.001)
        print(msg)
        p.send_message(msg)

    p.close();
  
main()
