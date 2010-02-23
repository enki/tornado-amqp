import struct
import cPickle

format = '=L'
format_size = struct.calcsize(format)
max_msg_size = 2 ** (format_size * 8) - 1

def dump(msg = None):
    data = cPickle.dumps(msg)
    msg_len = len(data)
    assert msg_len <= max_msg_size
    return struct.pack(format, msg_len) + data

class MessageSocket(object):

    def __init__(self, socket):
        self.socket = socket

    def write(self, msg):
        data = dump(msg)
        self.socket.sendall(data)

    def read(self):
        encoded_msg_size = self._socket_read(format_size)
        pickle_len, = struct.unpack(format, encoded_msg_size)
        encoded_msg      = self._socket_read(pickle_len)
        return cPickle.loads(encoded_msg)

    def _socket_read(self, bytes):
        data = ''
        while not len(data) == bytes:
            data += self.socket.recv(bytes - len(data))
        return data

class MessageStream(object):

    def __init__(self, iostream):
        self.iostream = iostream

    def write(self, msg, callback=None):
        data = dump(msg)
        self.iostream.write(data, callback)

    def read(self, callback):

        def read_pickle_len(data):
            (pickle_len,) = struct.unpack(format, data)
            self.iostream.read_bytes(pickle_len, read_pickle)

        def read_pickle(data):
            msg = cPickle.loads(data)
            try:
                callback(msg)
            except:
                raise #TODO FIXME

        self.iostream.read_bytes(format_size, read_pickle_len)
