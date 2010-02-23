import sys
import socket, os
import signal

from tornado import ioloop, iostream

class SlaveProcess(object):

    def __init__(self, callback, io_loop = None, max_buffer_size=104857600,
                 read_chunk_size=4096):
        self.callback = callback
        self.iostream = None
        self.pid = None
        self._io_loop = io_loop or ioloop.IOLoop.instance()
        self._max_buffer_size = max_buffer_size
        self._read_chunk_size = read_chunk_size

    def start(self):
        s_master, s_slave = socket.socketpair()
        self.pid = os.fork()
        if self.pid:
            print "pid",self.pid
            s_slave.close()
            print "master socket fd", s_master.fileno()
            self.iostream = iostream.IOStream(s_master,
                                              self._io_loop,
                                              self._max_buffer_size,
                                              self._read_chunk_size)
        else:
            s_master.close()
            print "slave socket fd", s_slave.fileno()
            self.callback(s_slave)
            sys.exit()

    def stop(self):
        os.kill(self.pid, signal.SIGTERM)
