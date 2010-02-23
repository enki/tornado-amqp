from amqplib.client_0_8 import Message as AmqplibMessage

from tamqp import slave
from tamqp import message

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

    def __init__(self, channel_factory, io_loop = None):
        self.channel_factory = channel_factory
        self.slave = slave.SlaveProcess(self._slave_main)
        self.slave.start()
        self.message_stream = message.MessageStream(self.slave.iostream)

class AmqpConsumer(AmqpSlave):

    def __init__(self, channel_factory, queue_name, callback, io_loop = None):
        self.queue_name = queue_name
        self.callback = callback
        super(AmqpConsumer, self).__init__(channel_factory, io_loop)
        self.message_stream.read(self._msg_callback)

    def _msg_callback(self, data):
        msg = data.to_amqplib_message()
        try:
            self.callback(msg)
        except:
            raise #TODO log
        self.message_stream.read(self._msg_callback)

    def _slave_main(self, socket):
        message_socket = message.MessageSocket(socket)
        ch = self.channel_factory()

        def forward_msg(msg):
            message_socket.write(_Message.from_amqplib_message(msg))

        ch.basic_consume(self.queue_name, callback=forward_msg, no_ack=True)
        while ch.callbacks:
            ch.wait()

class AmqpProducer(AmqpSlave):

    def publish(self, msg, exchange, callback=None):
        pickable_msg = _Message.from_amqplib_message(msg)
        self.message_stream.write((pickable_msg, exchange), callback)

    def _slave_main(self, socket):
        message_socket = message.MessageSocket(socket)
        ch = self.channel_factory()
        while True:
            msg, exchange = message_socket.read()
            ch.basic_publish(msg.to_amqplib_message(), exchange)
