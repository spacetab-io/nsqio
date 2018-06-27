import asyncio
import time
import logging
from . import consts
from .utils import retry_iterator, RdyControl
from .connection import create_connection
from .consts import TOUCH, REQ, FIN, RDY, CLS, MPUB, PUB, SUB, AUTH, DPUB

logger = logging.getLogger(__package__)


async def create_nsqd_connection(
        host='127.0.0.1', port=4150, loop=None, queue=None,
        heartbeat_interval=30000, feature_negotiation=True,
        tls_v1=False, snappy=False, deflate=False, deflate_level=6,
        consumer=False, sample_rate=0):
    """"
    param: host: host addr with no protocol. 127.0.0.1 
    param: port: host port 
    param: queue: queue where all the msg been put from the nsq 
    param: heartbeat_interval: heartbeat interval with nsq, set -1 to disable nsq heartbeat check
    params: snappy: snappy compress
    params: deflate: deflate compress  can't set True both with snappy
    """
    # TODO: add parameters type and value validation
    loop = loop or asyncio.get_event_loop()
    queue = queue or asyncio.Queue(loop=loop)
    conn = NsqdConnection(
        host=host, port=port, queue=queue,
        heartbeat_interval=heartbeat_interval,
        feature_negotiation=feature_negotiation,
        tls_v1=tls_v1, snappy=snappy, deflate=deflate,
        deflate_level=deflate_level,
        sample_rate=sample_rate, consumer=consumer, loop=loop)
    await conn.connect()
    return conn


class NsqdConnection:

    def __init__(self, host='127.0.0.1', port=4150, loop=None, queue=None,
                 heartbeat_interval=30000, feature_negotiation=True,
                 tls_v1=False, snappy=False, deflate=False, deflate_level=6,
                 sample_rate=0, consumer=False, max_in_flight=42):
        # TODO: add parameters type and value validation
        self._config = {
            "deflate": deflate,
            "deflate_level": deflate_level,
            "sample_rate": sample_rate,
            "snappy": snappy,
            "tls_v1": tls_v1,
            "heartbeat_interval": heartbeat_interval,
            'feature_negotiation': feature_negotiation,
        }

        self._host = host
        self._port = port
        self._conn = None
        self._loop = loop
        self._queue = queue or asyncio.Queue(loop=self._loop)

        self._status = consts.INIT
        self._loop.create_task(self.reconnect())

    async def connect(self):
        self._conn = await create_connection(self._host, self._port,
                                             self._queue, loop=self._loop)

        self._conn._on_message = self._on_message
        await self._conn.identify(**self._config)
        self._status = consts.CONNECTED
        if self.consumer:
            self._rdy_control.add_connection(self)

    async def reconnect(self):
        timeout_generator = retry_iterator(init_delay=0.1, max_delay=10.0)
        while True:
            if not (self._status == consts.CONNECTED):
                print('reconnect writer')
                try:
                    await self.connect()
                except ConnectionError:
                    logger.error("Can not connect to: {}:{} ".format(
                        self._host, self._port))
                else:
                    self._status = consts.CONNECTED
                t = next(timeout_generator)
            await asyncio.sleep(t, loop=self._loop)

    async def execute(self, command, *args, data=None):
        # if self._conn.closed:
        #     await self.reconnect()
        response = self._conn.execute(command, *args, data=data)
        return response

    @property
    def id(self):
        return self._conn.endpoint

    def close(self):
        self._conn.close()

    def __repr__(self):
        return '<Nsqd{}>'.format(self._conn.__repr__())
