#!/usr/bin/env python

import sys, signal
from tornado import ioloop, web, httpserver, options
from amqplib import client_0_8 as amqp_client
from amqplib.client_0_8 import Message
import tamqp

"""
RoundTrip benchmark.
For each single request:
1. publish is own id on the broker
2. the broker replies with the id
3. the request is "finished"

1. Run this example
2. ab -n 1000 -c 200 http://localhost:8001/round_trip
"""

XNAME="tornado_test_exchage"
QNAME="tornado_test_queue"
HOST="localhost:5672"

request_map = dict()

def finish_request(msg):
    req_id = msg.body
    request = request_map.pop(req_id)
    request.write(req_id)
    request.finish()

class RoundTripHandler(web.RequestHandler):

    @web.asynchronous
    def get(self):
        req_id = str(id(self))
        request_map[req_id] = self
        msg = Message(req_id)
        producer.publish(msg, exchange=XNAME)

def amqp_setup():
    conn = amqp_client.Connection(host=HOST, userid="guest", password="guest",
                                  virtual_host="/", insist=False)
    chan = conn.channel()
    chan.exchange_declare(exchange=XNAME, type="fanout", durable=True,
                          auto_delete=False)
    chan.queue_declare(queue=QNAME, durable=False, exclusive=False,
                       auto_delete=False)
    chan.queue_bind(queue=QNAME, exchange=XNAME)
    chan.close()
    conn.close()

def channel_factory():
    conn = amqp_client.Connection(host=HOST, userid="guest", password="guest",
                                  virtual_host="/", insist=False)
    return conn.channel()

def main():
    signal.signal(signal.SIGTERM, lambda sig, frame: sys.exit(0))
    global listeners, consumer, producer
    options.parse_command_line()
    amqp_setup()
    consumer = tamqp.AmqpConsumer(channel_factory, QNAME, finish_request)
    producer = tamqp.AmqpProducer(channel_factory)

    application = web.Application([
        (r"/round_trip", RoundTripHandler),
    ])

    http_server = httpserver.HTTPServer(application)
    http_server.listen(8001)
    ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    try:
        main()
    except (SystemExit, KeyboardInterrupt):
        consumer.stop()
        producer.stop()
