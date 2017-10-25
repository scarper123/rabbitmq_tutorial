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
from setting import MQ_CONFIG


def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)


def consumer_callback(ch, method, props, body):
    n = int(body)
    print(' [.] fib(%s)' % n)
    response = fib(n)
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(
                         correlation_id=props.correlation_id),
                     body=str(response))

    ch.basic_ack(delivery_tag=method.delivery_tag)


conn = pika.BlockingConnection(
    parameters=pika.ConnectionParameters(**MQ_CONFIG))
channel = conn.channel()

queue = channel.queue_declare('rpc_queue')
channel.basic_qos(prefetch_count=1)
channel.basic_consume(consumer_callback, 'rpc_queue')


print(" [x] Awaiting RPC requests")
channel.start_consuming()
