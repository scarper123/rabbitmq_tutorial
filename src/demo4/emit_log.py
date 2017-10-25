#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2017-10-20 13:18:34
# @Author  : Shanming Liu

import os
import sys
import random

BASEDIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(BASEDIR)

print(SRC_DIR)
sys.path.append(SRC_DIR)

import pika
from setting import QUEUE_SETTING, MQ_CONFIG

# message = ' '.join(sys.argv[1:]) or 'Hello World!'
KEYS = [item for items in QUEUE_SETTING for item in items]

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
channel.exchange_declare(exchange='direct_log')
# channel.exchange_declare(exchange='emit_log', exchange_type='fanout')
# queue = channel.queue_declare(queue='hello', durable=True)

for message in messages:
    routing_key = random.choice(KEYS)
    print('Send message <%r> with routing_key <%r>.' % (message, routing_key))
    channel.basic_publish(exchange='direct_log',
                          routing_key=routing_key,
                          body=message)

    # print(' [x] Sent %r' % message)
connection.close()
