from amqplib.client_0_8.serialization import GenericContent

from tamqp import slave
from tamqp import message

GenericContent.__getstate__ = lambda self: False #to fix Message pickability

class AmqpSlave(object):

    def __init__(self, channel_factory, io_loop = None):
        self.channel_factory = channel_factory
        self.slave = slave.SlaveProcess(self._slave_main)
        self.slave.start()
        self.message_stream = message.MessageStream(self.slave.iostream)

class AmqpConsumer(AmqpSlave):

    def __init__(self, channel_factory, callback, io_loop = None):
        super(AmqpConsumer, self).__init__(channel_factory, io_loop)
        self.message_stream.read(self._msg_callback)
        self.callback = callback

    def _msg_callback(self, data):
        try:
            self.callback(data)
        except:
            pass #TODO log
        self.message_stream.read(self._msg_callback)

    def _slave_main(self, socket):
        message_socket = message.MessageSocket(socket)
        ch = self.channel_factory()

        def forward_msg(msg):
            message_socket.write(msg)

        ch.basic_consume('consumer', callback=forward_msg)
        while ch.callbacks:
            ch.wait()

class AmqpProducer(AmqpSlave):

    def publish(self, msg, exchange, callback=None):
        self.message_stream.write((msg, exchange), callback)

    def _slave_main(self, socket):
        message_socket = message.MessageSocket(socket)
        ch = self.channel_factory()
        while True:
            print "producer - preparing for socket read"
            msg, exchange = message_socket.read()
            print "produrer - socket read: %s %s" % (msg, exchange)
            ch.basic_publish(msg, exchange)
