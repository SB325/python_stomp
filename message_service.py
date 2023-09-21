import stomp;
import queue;
import sys;
from message_producer import message_producer
from message_consumer import message_consumer

class message_service():
    def __init__(self, producer_host, producer_port, consumer_host, consumer_port, consumer_subscriberID, producer_topic, consumer_topic, message_queue):
        consumerTopicFull = '/topic/'+consumer_topic
        self.consumer = message_consumer(consumerTopicFull, consumer_subscriberID, True)
        self.consumer.start(consumer_host, consumer_port)
        self.consumer.subscribe()
        producerTopicFull = '/topic/'+producer_topic
        self.producer = message_producer(producer_host, producer_port, producerTopicFull, True)

    def send_message(self, message):
        self.producer.send_message(message)

    def get_queue(self):
        return self.consumer.get_queue()
