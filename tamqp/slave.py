import sys
import socket, os
import signal, logging

from tornado import ioloop, iostream

logger = logging.getLogger('tamqp.slave')

class SlaveProcess(object):

    def __init__(self, slave_main, io_loop=None, max_buffer_size=104857600,
                 read_chunk_size=4096):
        self.slave_main = slave_main
        self.iostream = None
        self.pid = None
        self._io_loop = io_loop or ioloop.IOLoop.instance()
        self._max_buffer_size = max_buffer_size
        self._read_chunk_size = read_chunk_size

    def start(self):
        s_master, s_slave = socket.socketpair()
        self.pid = os.fork()
        if self.pid:
            logger.info('slave child (pid=%d) started', self.pid)
            s_slave.close()
            self.iostream = iostream.IOStream(s_master,
                                              self._io_loop,
                                              self._max_buffer_size,
                                              self._read_chunk_size)
        else:
            try:
                s_master.close()
            except os.error:
                pass
            try:
                self.slave_main(s_slave)
            except:
                logger.error('error in slave_main', exec_info=True)
            sys.exit()

    def stop(self):
        os.kill(self.pid, signal.SIGTERM)
