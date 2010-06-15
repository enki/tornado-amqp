import logging
from amqplib.client_0_8 import Message as AmqplibMessage

from tamqp import slave
from tamqp import message

logger = logging.getLogger('tamqp')

#proxy class for amqplib.client_0_8.Message.
#until http://code.google.com/p/py-amqplib/issues/detail?id=15 is fixed
class _Message(object):

    def __init__(self, body, children=None, **properties):
        self.body = body
        self.properties = properties

    def to_amqplib_message(self):
        return AmqplibMessage(self.body, **self.properties)

    @classmethod
    def from_amqplib_message(cls, msg):
        return cls(msg.body, **msg.properties)

class AmqpSlave(object):

    def __init__(self, channel_factory, on_start=None, io_loop=None):
        assert on_start is None or callable(on_start)
        self.on_start = on_start
        self.channel_factory = channel_factory
        self.slave = slave.SlaveProcess(self._slave_main, io_loop)
        self.slave.start()
        self.message_stream = message.MessageStream(self.slave.iostream)

    def stop(self):
        self.message_stream.close()
        self.slave.stop()

    def _slave_main(self, socket):
        if self.on_start:
            self.on_start()
        message_socket = message.MessageSocket(socket)
        ch = self.channel_factory()
        try:
            self._slave_impl(ch, message_socket)
        finally:
            if ch:
                conn = ch.connection
                ch.close()
                if conn:
                    conn.close()
            logger.info("closed amqp connection")

class AmqpConsumer(AmqpSlave):

    def __init__(self, channel_factory, queue_name, callback, on_start=None,
                 io_loop=None):
        self.queue_name = queue_name
        self.callback = callback
        super(AmqpConsumer, self).__init__(channel_factory,
                                           on_start=on_start,
                                           io_loop=io_loop)
        self.message_stream.read(self._msg_callback)

    def _msg_callback(self, data):
        msg = data.to_amqplib_message()
        try:
            self.callback(msg)
        except (SystemExit, KeyboardInterrupt):
            raise
        except:
            logger.error('error in amqp consumer callback', exc_info=True)
        self.message_stream.read(self._msg_callback)

    def _slave_impl(self, ch, message_socket):
        def forward_msg(msg):
            message_socket.write(_Message.from_amqplib_message(msg))

        ch.basic_consume(self.queue_name, callback=forward_msg, no_ack=True)
        while ch.callbacks:
            ch.wait()

class AmqpProducer(AmqpSlave):

    def publish(self, msg, exchange='', routing_key = '', mandatory=False,
                immediate=False, callback=None):
        pickable_msg = _Message.from_amqplib_message(msg)
        params = dict(exchange=exchange, routing_key=routing_key,
                      mandatory=mandatory, immediate=immediate)
        self.message_stream.write((pickable_msg, params), callback)

    def _slave_impl(self, channel, message_socket):
        while True:
            msg, params = message_socket.read()
            channel.basic_publish(msg.to_amqplib_message(), **params)
