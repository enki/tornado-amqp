#!/usr/bin/env python

import sys, signal
from tornado import ioloop, web, httpserver, options
from amqplib import client_0_8 as amqp_client
from amqplib.client_0_8 import Message
import tamqp

"""
Thanks to Wil Tan (http://dready.org/) for the demo application
Refactored by Paul Bohm (http://paulbohm.com/)

1. Run this example
2. Open http://localhost:8080/monitor in browser to monitor message pubblication
3. Publish a messages using curl
$ curl http://localhost:8080/pub?q=hello
"""

XNAME="tornado_test_exchage"
QNAME="tornado_test_queue"
HOST="localhost:5672"

BROKER_USER = "myuser"
BROKER_PASSWORD = "mypassword"
BROKER_VHOST = "myvhost"

SETUPPED = False
PRODUCER_SINGLETON = None

listeners = []
def notify_listeners(msg):
    for l in list(listeners):
        l(msg)

def amqp_setup():
    conn = amqp_client.Connection(host=HOST, userid=BROKER_USER, password=BROKER_PASSWORD,
                                  virtual_host=BROKER_VHOST, insist=False)
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

def prepare():
    global SETUPPED
    if not SETUPPED:
        amqp_setup()
        SETUPPED = True

def subscribe(queue, callback):
    prepare()
    consumer = tamqp.AmqpConsumer(channel_factory, queue, callback)
    return consumer

def make_producer():
    prepare()
    global PRODUCER_SINGLETON
    if not PRODUCER_SINGLETON:
        producer = tamqp.AmqpProducer(channel_factory)
    else:
        producer = PRODUCER_SINGLETON
    return producer
    
class MonitorHandler(web.RequestHandler):

    @web.asynchronous
    def get(self):
        self.request.connection.stream.set_close_callback(self.on_connection_close)
        listeners.append(self.message_received)

    def message_received(self, msg):
        self.write(msg.body)
        try:
            self.finish()
        except:
            pass # well, either way we are done. no time for tears.

    def on_connection_close(self):
        listeners.remove(self.message_received)

class PubHandler(web.RequestHandler):
    def get(self):
        self.write("publishing...")
        msg = Message(self.get_argument("q"))
        make_producer().publish(msg, exchange=XNAME)

def main():
    signal.signal(signal.SIGTERM, lambda sig, frame: sys.exit(0))
    # global listeners, consumer, producer
    options.parse_command_line()
    
    application = web.Application([
        (r"/monitor", MonitorHandler),
        (r"/pub",     PubHandler),
    ])

    http_server = httpserver.HTTPServer(application)
    http_server.listen(8080)
    
    subscribe(QNAME, notify_listeners)
    
    ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    # try:
    main()
    # except (SystemExit, KeyboardInterrupt):
    #     consumer.stop()
    #     producer.stop()
