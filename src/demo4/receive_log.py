#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2017-10-20 14:44:12
# @Author  : Shanming Liu

import os
import sys
import time
from multiprocessing.pool import Pool
# from gevent.pool import Pool
# from gevent import monkey
# monkey.patch_all()

BASEDIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(BASEDIR)
sys.path.append(SRC_DIR)

import pika
from setting import QUEUE_SETTING, MQ_CONFIG


def callback(ch, method, properties, body):
    print(" [x] %r Received %r" % (method.routing_key, body))
    # time.sleep(5)
    # time.sleep(body.count(b'.'))
    # print(" [x] %r Done" % os.getpid())
    # ch.base_ack(delivery_tag = method.delivery_tag)


def receiver(*keys):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(**MQ_CONFIG))
    channel = connection.channel()
    channel.exchange_declare(exchange='direct_log', exchange_type='direct')
    # channel.exchange_declare(exchange='emit_log', exchange_type='fanout')
    # durable=True
    # When RabbitMQ quits or crashes it will forget the queues and messages unless you tell it not to.
    # Two things are required to make sure that messages aren't lost: we need to mark both the queue and messages as durable.
    queue = channel.queue_declare(exclusive=True)
    queue_name = queue.method.queue

    print(' [x] %r Bind keys %r' % (os.getpid(), keys))
    for key in keys:
        channel.queue_bind(queue_name, 'direct_log', routing_key=key)
    # This tells RabbitMQ not to give more than one message to a worker at a time
    # channel.basic_qos(prefetch_count=1)
    # channel.basic_consume(callback, queue='hello')
    # no_ack=True not wait Worker callback signal.
    # if not you should ack manually.
    channel.basic_consume(callback, queue=queue_name, no_ack=True)

    print(' [x] %r Waiting for message. To exit press CTRL+C' % os.getpid())
    channel.start_consuming()


# By default, RabbitMQ will send each message to the next consumer, in sequence.
# On average every consumer will get the same number of messages. This way of distributing messages is called round-robin.
pool = Pool(2)
res = []
for x, keys in zip(range(2), QUEUE_SETTING):
    res.append(pool.apply_async(receiver, args=keys))

pool.close()
pool.join()
map(lambda job: job.get(), res)
