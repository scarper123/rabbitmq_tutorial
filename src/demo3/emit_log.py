#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2017-10-20 13:18:34
# @Author  : Shanming Liu

import os
import sys

BASEDIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(BASEDIR)

print(SRC_DIR)
sys.path.append(SRC_DIR)

import pika
from setting import MQ_CONFIG

# message = ' '.join(sys.argv[1:]) or 'Hello World!'

messages = [
    'First message.',
    'Second message..',
    'Third message...',
    'Fourth message....',
    'Fifth message.....',
]

connection = pika.BlockingConnection(
    pika.ConnectionParameters(**MQ_CONFIG))
channel = connection.channel()
# channel.exchange_declare(exchange='emit_log', exchange_type='')
channel.exchange_declare(exchange='emit_log', exchange_type='fanout')
# queue = channel.queue_declare(queue='hello', durable=True)

for message in messages:
    channel.basic_publish(exchange='emit_log',
                          routing_key='',
                          body=message)

    print(' [x] Sent %r' % message)
connection.close()
