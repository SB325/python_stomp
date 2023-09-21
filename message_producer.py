import stomp;
import logging;
import getopt;
import sys;

class message_producer:

    def __init__(self, host, port, topic, verbose=False):
        self.connection = None
        self.destination = topic
        self.user = None
        self.passcode = None
        self.verbose = verbose
        self.broker(host, port)

    def broker(self, host, port):
        try:
            if (host is not None and port is not None):
                self.connection = stomp.Connection([(host, port)], auto_content_length=False)
            else:
                print('error: cannot create connection')

            #self.connection.start()

            if (self.user is None or self.passcode is None):
                self.connection.connect()
            else:
                self.connection.connect(self.user, self.passcode)

        except Exception as e:
            print(e)

    def send_message(self, jsonStr):
        if (self.connection is not None):
            if (self.verbose):
                print("message: ", jsonStr)
            self.connection.send(destination=self.destination, body=jsonStr)
        else:
            print('error: connection does not exist')

    def close(self):
        if (self.connection is not None):
            self.connection.disconnect()

