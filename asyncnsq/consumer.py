import asyncio
import random
from collections import deque
import time
import logging
from asyncnsq.http import NsqLookupd
from asyncnsq.nsq import create_nsq
from asyncnsq.utils import RdyControl
logger = logging.getLogger()


class NsqConsumer:
    """Experiment purposes"""

    def __init__(self, nsqd_tcp_addresses=None, lookupd_http_addresses=None,
                 max_in_flight=42, loop=None):

        self._nsqd_tcp_addresses = nsqd_tcp_addresses or []
        self._lookupd_http_addresses = lookupd_http_addresses or []

        self._max_in_flight = max_in_flight
        self._loop = loop or asyncio.get_event_loop()
        self._queue = asyncio.Queue(loop=self._loop)

        self._connections = {}

        self._idle_timeout = 10

        self._rdy_control = None
        self._max_in_flight = max_in_flight

        self._is_subscribe = False
        self._redistribute_timeout = 5  # sec
        self._lookupd_poll_time = 30  # sec
        self.topic = None
        self._rdy_control = RdyControl(idle_timeout=self._idle_timeout,
                                       max_in_flight=self._max_in_flight,
                                       loop=self._loop)

    async def connect(self):
        if self._lookupd_http_addresses:
            """
            because lookupd must find a topic to find nsqds
            so, lookupd connect changed in to subscribe func
            """
            pass
        if self._nsqd_tcp_addresses:
            for host, port in self._nsqd_tcp_addresses:
                conn = await create_nsq(host, port, queue=self._queue,
                                        loop=self._loop)
            self._connections[conn.id] = conn
            self._rdy_control.add_connections(self._connections)

    async def _poll_lookupd(self, host, port):
        nsqlookup_conn = NsqLookupd(host, port, loop=self._loop)
        try:
            res = await nsqlookup_conn.lookup(self.topic)
            logger.info('lookupd response')
            logger.info(res)
        except Exception as tmp:
            logger.error(tmp)
            logger.exception(tmp)

        for producer in res['producers']:
            host = '127.0.0.1'
            # producer['broadcast_address']
            port = producer['tcp_port']
            tmp_id = "tcp://{}:{}".format(host, port)
            if tmp_id not in self._connections:
                logger.info(('host, port', host, port))
                conn = await create_nsq(host, port, queue=self._queue,
                                        loop=self._loop)
                print('conn.id:', conn.id)
                self._connections[conn.id] = conn
                self._rdy_control.add_connection(conn)
        nsqlookup_conn.close()

    async def lookupd_task_done_sub(self, topic, channel):
        for conn in self._connections.values():
            result = await conn.sub(topic, channel)
        self._redistribute_task = asyncio.Task(self._redistribute(),
                                               loop=self._loop)

    async def subscribe(self, topic, channel):
        self.topic = topic
        self._is_subscribe = True
        if self._lookupd_http_addresses:
            await self._lookupd()
        for conn in self._connections.values():
            result = await conn.sub(topic, channel)
        self._redistribute_task = asyncio.Task(self._redistribute(),
                                               loop=self._loop)

    def wait_messages(self):
        if not self._is_subscribe:
            raise ValueError('You must subscribe to the topic first')

        while self._is_subscribe:
            fut = asyncio.ensure_future(self._queue.get(), loop=self._loop)
            yield fut

    def is_starved(self):
        conns = self._connections.values()
        return any(conn.is_starved() for conn in conns)

    async def _redistribute(self):
        while self._is_subscribe:
            self._rdy_control.redistribute()
            await asyncio.sleep(self._redistribute_timeout,
                                loop=self._loop)

    async def _lookupd(self):
        host, port = random.choice(self._lookupd_http_addresses)
        result = await self._poll_lookupd(host, port)
