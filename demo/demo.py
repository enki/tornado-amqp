#!/usr/bin/env python

from tornado import ioloop, web, httpserver
from amqplib import client_0_8 as amqp_client
from amqplib.client_0_8 import Message
import tamqp

"""
Thnaks to Wil Tan (http://dready.org/) for the demo application

1. Run this example
2. Open http://localhost:8080/monitor in browser to monitor message pubblication
3. Publish a messages using curl
$ curl http://localhost:8080/pub?q=hello
"""

XNAME="tornado_test_exchage"
QNAME="tornado_test_queue"
HOST="localhost:5672"

class MonitorHandler(web.RequestHandler):

    def stop_monitoring(self, msg):
        listeners.remove(self.stop_monitoring)
        self.write(msg.body)
        self.finish()

    @web.asynchronous
    def get(self):
        listeners.append(self.stop_monitoring)

class PubHandler(web.RequestHandler):
    def get(self):
        self.write("publishing...")
        msg = Message(self.get_argument("q"))
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

listeners = []
def notify_listeners(msg):
    for l in list(listeners):
        l(msg)

def main():
    global listeners, consumer, producer
    amqp_setup()
    consumer = tamqp.AmqpConsumer(channel_factory, QNAME, notify_listeners)
    producer = tamqp.AmqpProducer(channel_factory)

    application = web.Application([
        (r"/monitor", MonitorHandler),
        (r"/pub",     PubHandler),
    ])

    http_server = httpserver.HTTPServer(application)
    http_server.listen(8080)
    ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt, SysExit:
        consumer.slave.stop()
        producer.slave.stop()
