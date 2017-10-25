#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2017-10-20 14:44:12
# @Author  : Shanming Liu

import os
import sys

BASEDIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(BASEDIR)
sys.path.append(SRC_DIR)

import pika
from setting import MQ_CONFIG


connection = pika.BlockingConnection(
    pika.ConnectionParameters(**MQ_CONFIG))
channel = connection.channel()
queue = channel.queue_declare(queue='hello')


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)


channel.basic_consume(callback, queue='hello', no_ack=True)

print(' [x] Waiting for message. To exit press CTRL+C')
channel.start_consuming()
