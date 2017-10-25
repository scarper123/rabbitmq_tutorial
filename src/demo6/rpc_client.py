#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2017-10-20 14:44:12
# @Author  : Shanming Liu

import os
import sys
import uuid

BASEDIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(BASEDIR)
sys.path.append(SRC_DIR)

import pika
from setting import MQ_CONFIG


class FibClient(object):
    """docstring for FibClient"""

    def __init__(self):
        super(FibClient, self).__init__()

        self.conn = pika.BlockingConnection(
            parameters=pika.ConnectionParameters(**MQ_CONFIG))
        self.channel = self.conn.channel()

        self.callback_queue = self.channel.queue_declare(
            exclusive=True).method.queue
        self.channel.basic_qos(prefetch_count=1)

        self.channel.basic_consume(
            self.do_response, queue=self.callback_queue, no_ack=True)

    def do_response(self, ch, method, props, body):
        if props.correlation_id == self.correlation_id:
            self.response = body

    def call(self, n):
        self.response = None
        self.correlation_id = str(uuid.uuid4())

        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.correlation_id,
            ),
            body=str(n))

        while self.response is None:
            self.conn.process_data_events()
        return int(self.response)


if __name__ == '__main__':

    if len(sys.argv) > 1:
        args = sys.argv[1:]
    else:
        args = [5]

    client = FibClient()
    for arg in args:
        response = client.call(arg)
        print('Calculate fib(%s)=%s' % (arg, response))
