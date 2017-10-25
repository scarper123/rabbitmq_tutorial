#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2017-10-20 14:44:12
# @Author  : Shanming Liu

import os
import sys
import time
from multiprocessing.pool import Pool

BASEDIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(BASEDIR)
sys.path.append(SRC_DIR)

import pika
from setting import MQ_CONFIG


def callback(ch, method, properties, body):
    print(" [x] %r Received %r" % (os.getpid(), body))
    # time.sleep(5)
    # time.sleep(body.count(b'.'))
    # print(" [x] %r Done" % os.getpid())
    # ch.base_ack(delivery_tag = method.delivery_tag)


def receiver():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(**MQ_CONFIG))
    channel = connection.channel()
    # durable=True
    # When RabbitMQ quits or crashes it will forget the queues and messages unless you tell it not to. 
    # Two things are required to make sure that messages aren't lost: we need to mark both the queue and messages as durable.
    queue = channel.queue_declare(queue='hello', durable=True)

    # This tells RabbitMQ not to give more than one message to a worker at a time
    channel.basic_qos(prefetch_count=1)
    # channel.basic_consume(callback, queue='hello')
    # no_ack=True not wait Worker callback signal.
    # if not you should ack manually.
    channel.basic_consume(callback, queue='hello', no_ack=True)

    print(' [x] %r Waiting for message. To exit press CTRL+C' % os.getpid())
    channel.start_consuming()


# By default, RabbitMQ will send each message to the next consumer, in sequence. 
# On average every consumer will get the same number of messages. This way of distributing messages is called round-robin.
pool = Pool(2)
for x in range(2):
    pool.apply_async(receiver)

pool.close()
pool.join()
