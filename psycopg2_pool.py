from __future__ import with_statement
from gevent.queue import Queue
from gevent.socket import wait_read, wait_write
from psycopg2 import extensions, OperationalError

import logging
import psycopg2

logger = logging.getLogger(__name__)

def gevent_wait_callback(conn, timeout=None):
    """A wait callback useful to allow gevent to work with Psycopg."""
    while 1:
        state = conn.poll()
        if state == extensions.POLL_OK:
            break
        elif state == extensions.POLL_READ:
            wait_read(conn.fileno(), timeout=timeout)
        elif state == extensions.POLL_WRITE:
            wait_write(conn.fileno(), timeout=timeout)
        else:
            raise OperationalError(
                "Bad result from poll: %r" % state)


extensions.set_wait_callback(gevent_wait_callback)

class DatabaseConnectionPool(object):
    def __init__(self, maxsize=10):
        if not isinstance(maxsize, (int, long)):
            raise TypeError('Expected integer, got %r' % (maxsize, ))
        self.maxsize = maxsize
        self.pool = Queue()
        self.size = 0
        self.gets = 0
        self.puts = 0

    def get(self):
        pool = self.pool
        self.gets += 1
        pool_size = pool.qsize()
        if self.size >= self.maxsize or pool_size:
            return pool.get()
        else:
            self.size += 1
            try:
                new_item = self.create_connection()
            except Exception, e:
                logger.error(e.message)
                self.size -= 1
                raise
            return new_item

    def put(self, item):
        self.pool.put(item)
        self.puts += 1

    def closeall(self):
        while not self.pool.empty():
            conn = self.pool.get_nowait()
            try:
                conn.close()
            except Exception, e:
                logger.error(e.message)

class PostgresConnectionPool(DatabaseConnectionPool):
    def __init__(self, database, *args, **kwargs):
        self.database=database
        maxsize = kwargs.pop('maxsize', None)
        self.args = args
        self.kwargs = kwargs
        DatabaseConnectionPool.__init__(self, maxsize)

    def create_connection(self):
        conn = psycopg2.connect(database=self.database, **self.kwargs)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        return conn
