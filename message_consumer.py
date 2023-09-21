import stomp

class message_listener(stomp.ConnectionListener):
    def __init__(self, conn, msg_queue, ver, m=None):
        self.msg_queue = msg_queue
        self.max_msgs = m
        self.msg_count = 0
        self.verbose = ver
        self.connection = conn

    def on_error(self, message): 
        print('Error in message_listener')
 
    def on_message(self, message): 
        print('MESSAGE RECEIEVED!')
        try:
            if (message is not None):
                if (self.verbose):
                    print(message)
                if (self.max_msgs is not None and (self.msg_count >= int(self.max_msgs))):
                    if (self.verbose):
                        print("Maximum number of received messages reached")
                    self.stop()
                else:
                    self.msg_count = self.msg_count + 1;
                    self.msg_queue.put(message)
            else:
                print("Received message with nothing in it")
        except Exception as e:
            print("Unable to add message to the queue")
            print(e)

    def get_queue(self):
        if(self.msg_queue is None):
            print('self.msg_queue is None')
        return self.msg_queue

    def stop(self):
        if (self.connection is not None):
            print("Disconnecting")
            self.connection.disconnect()


class message_consumer():

    def __init__(self, topic, idnum, msg_queue, v, m=None):
        self.connection = None
        self.listener = None
        self.destination = topic
        self.msg_queue = msg_queue
        self.user = None
        self.passcode = None
        self.max_msgs = m
        self.sub = idnum
        self.verbose = v

    def start(self, host, port):
        try:
            if (host is not None and port is not None):
                if (self.verbose):
                    print('creating consumer connection')
                self.connection = stomp.Connection12([(host, port)], auto_content_length=False)
            else:
                print('Error: problem creating the connection')
      
            if (self.verbose):
                print('Setting Listener:')
            self.listener = message_listener(self.connection, self.msg_queue, self.verbose, self.max_msgs)  
            self.connection.set_listener('', self.listener);
            if (self.verbose):
                print('Listener is set')
            #self.connection.start()
 
            if (self.user is None or self.passcode is None):
                self.connection.connect();
            else:
                self.connection.connect(self.user, self.passcode);
            
            print('Connection Established on Host: ' + host + ', Port: ' + str(port) + ', Topic: ' \
                 + ' + '.join(self.destination) + ', SubscriberID: ' + str(self.sub) );
        except Exception as e:
            print('Error on connection ' + str(e));
    
    def subscribe(self):
        if (self.connection is not None):
            self.connection.subscribe(destination = self.destination[0], id=self.sub, ack='auto')
            if (self.verbose):
                print('Subscribed to ' + self.destination[0])
        else:
            print('Error: unable to subscribe, connection is None')

    def get_queue(self):
        if(self.listener is None and self.verbose):
            print ('self.listener is None')

        return self.listener.get_queue()
    
    def stop(self):
        if(self.connection is not None):
            self.connection.disconnect()

    def verbose(self, val):
        self.verbose = val



