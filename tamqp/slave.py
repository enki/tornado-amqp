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
            exit_code = 0
            try:
                self.slave_main(s_slave)
            except KeyboardInterrupt:
                logger.info('SIGINT caught')
            except SystemExit:
                pass
            except:
                logger.error('error in slave_main', exc_info=True)
                exit_code = 1
            logger.info('slave exiting ...')
            os._exit(exit_code)

    def stop(self):
        logger.info('stopping slave child (pid=%d)', self.pid)
        if self.pid:
            os.kill(self.pid, signal.SIGINT)
        self.pid = None
