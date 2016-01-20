#!/usr/bin/env python
import pika
import json
import datetime
import time

class Consumer(object):
    def __init__(self, host, username, password):
        self.host = host
        self.userName = username
        self.password = password

    # callback provided for rabbitmq eventing consumer.
    def callback(self, ch, method, properties, body):
        st =  body.decode('utf-8', 'replace')
        jsn = json.loads(st)
        print(jsn['timestamp'] + ',' + str(datetime.datetime.fromtimestamp(time.time())))

    # establish connection to the broker and return if channel is open.
    def setupChannel(self,host):
        params = pika.ConnectionParameters(host=self.host, credentials=pika.PlainCredentials(self.userName,self.password))
        connection = pika.BlockingConnection(params)
        self.channel =  connection.channel()
        return self.channel.is_open

    def setupQueueAndConsume(self,queueName, exchange, routingKey):
        # Declare a queue to hold the messages we are interested in
        queue = self.channel.queue_declare(queue=queueName)

        # setup this queue with teh specified meta bindings
        self.channel.queue_bind(exchange=exchange,queue=queueName, routing_key=routingKey)
        self.channel.basic_consume(self.callback,
                              queue=queue.method.queue,
                              no_ack=True)
        self.channel.start_consuming()

    def startListeningOnStream(self):
        print('setting up a channel' + str(self.setupChannel(self.host)))
        print('beginning consumtion of stream phoenix.api.stream with binding "#"')
        self.setupQueueAndConsume('phoenix.api.stream.dbz_aapl', 'exchange_stocks', 'phoenix.api.stream.aapl')
	# def startListeningStreaming(self, host):
	#     print('setting up a channel' + self.setupChannel(host))
	# 	print('beginning consumtion of stream phoenix.api.stream with binding "#"')
	# 	self.setupQueueAndConsume('phoenix.api.stream', 'exchange_stocks', '#')
