import asyncio
from . import consts
import time
from .log import logger
from .utils import retry_iterator, RdyControl
from .connection import create_connection
from .consts import TOUCH, REQ, FIN, RDY, CLS, MPUB, PUB, SUB, AUTH, DPUB


async def create_nsq(host='127.0.0.1', port=4150, loop=None, queue=None,
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
    conn = Nsq(host=host, port=port, queue=queue,
               heartbeat_interval=heartbeat_interval,
               feature_negotiation=feature_negotiation,
               tls_v1=tls_v1, snappy=snappy, deflate=deflate,
               deflate_level=deflate_level,
               sample_rate=sample_rate, consumer=consumer, loop=loop)
    await conn.connect()
    return conn


class Nsq:

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
        self._reconnect = True
        self._rdy_state = 0
        self._last_message = None

        self._on_rdy_changed_cb = None
        self._last_rdy = 0
        self.consumer = consumer
        if self.consumer:
            self._idle_timeout = 10
            self._max_in_flight = max_in_flight
            self._rdy_control = RdyControl(idle_timeout=self._idle_timeout,
                                           max_in_flight=self._max_in_flight,
                                           loop=self._loop)

    async def connect(self):
        self._conn = await create_connection(self._host, self._port,
                                             self._queue, loop=self._loop)

        self._conn._on_message = self._on_message
        await self._conn.identify(**self._config)
        self._status = consts.CONNECTED
        if self.consumer:
            self._rdy_control.add_connection(self)

    def _on_message(self, msg):
        # should not be coroutine
        # update connections rdy state
        self.rdy_state = int(self.rdy_state) - 1

        self._last_message = time.time()
        if self._on_rdy_changed_cb is not None:
            self._on_rdy_changed_cb(self.id)
        return msg

    @property
    def rdy_state(self):
        return self._rdy_state

    @rdy_state.setter
    def rdy_state(self, value):
        self._rdy_state = value

    @property
    def in_flight(self):
        return self._conn.in_flight

    @property
    def last_message(self):
        return self._last_message

    async def reconnect(self):
        timeout_generator = retry_iterator(init_delay=0.1, max_delay=10.0)
        while not (self._status == consts.CONNECTED):
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
        if self._status <= consts.CONNECTED and self._reconnect:
            await self.reconnect()

        response = self._conn.execute(command, *args, data=data)
        return response

    @property
    def id(self):
        return self._conn.endpoint

    def wait_messages(self):
        # print('wait_messages')
        while True:
            future = self._queue.get()
            print(future, type(future))
            aa = yield from future
            while not aa.done():
                print('not done')
                pass
            print(aa.result())
            yield aa.result()

    async def auth(self, secret):
        """

        :param secret:
        :return:
        """
        return await self._conn.execute(AUTH, data=secret)

    async def sub(self, topic, channel):
        """

        :param topic:
        :param channel:
        :return:
        """
        self._is_subscribe = True

        return await self._conn.execute(SUB, topic, channel)

    async def _redistribute(self):
        while self._is_subscribe:
            self._rdy_control.redistribute()
            await asyncio.sleep(60,
                                loop=self._loop)

    async def pub(self, topic, message):
        """

        :param topic:
        :param message:
        :return:
        """
        return await self._conn.execute(PUB, topic, data=message)

    async def dpub(self, topic, delay_time, message):
        """

        :param topic:
        :param message:
        :param delay_time: delayed time in millisecond
        :return:
        """
        if not delay_time or delay_time is None:
            delay_time = 0
        return await self._conn.execute(DPUB, topic, delay_time, data=message)

    async def mpub(self, topic, message, *messages):
        """

        :param topic:
        :param message:
        :param messages:
        :return:
        """
        msgs = [message] + list(messages)
        return await self._conn.execute(MPUB, topic, data=msgs)

    async def rdy(self, count):
        """

        :param count:
        :return:
        """
        if not isinstance(count, int):
            raise TypeError('count argument must be int')

        self._last_rdy = count
        self.rdy_state = count
        return await self._conn.execute(RDY, count)

    async def fin(self, message_id):
        """

        :param message_id:
        :return:
        """
        return await self._conn.execute(FIN, message_id)

    async def req(self, message_id, timeout):
        """

        :param message_id:
        :param timeout:
        :return:
        """
        return await self._conn.execute(REQ, message_id, timeout)

    async def touch(self, message_id):
        """

        :param message_id:
        :return:
        """
        return await self._conn.execute(TOUCH, message_id)

    async def cls(self):
        """

        :return:
        """
        await self._conn.execute(CLS)
        self.close()

    def close(self):
        self._conn.close()

    def is_starved(self):

        if self._queue.qsize():
            starved = False
        else:
            starved = (self.in_flight > 0 and
                       self.in_flight >= (self._last_rdy * 0.85))
        return starved

    def __repr__(self):
        return '<Nsq{}>'.format(self._conn.__repr__())
