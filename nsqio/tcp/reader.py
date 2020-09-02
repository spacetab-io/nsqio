from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nsqio.tcp.connection import TcpConnection

import asyncio
import random
import logging
import time

from functools import partial

from nsqio.http import NsqLookupd
from nsqio.tcp.reader_rdy import RdyControl
from nsqio.tcp.connection import create_connection
from nsqio.tcp.consts import SUB, RDY, CLS
from nsqio.utils import get_logger, get_host_and_port, retry_iterator, get_version


logger = get_logger()


async def create_reader(
    nsqd_tcp_addresses=None,
    loop=None,
    max_in_flight=42,
    lookupd_http_addresses=None,
    **kwargs,
):
    """"
    initial function to get consumer
    param: nsqd_tcp_addresses: tcp addrs with no protocol.
        such as ['127.0.0.1:4150','182.168.1.1:4150']
    param: max_in_flight: number of messages get but not finish or req
    param: lookupd_http_addresses: first priority.if provided nsqd will neglected
    """
    loop = loop or asyncio.get_event_loop()
    if lookupd_http_addresses:
        reader = Reader(
            lookupd_http_addresses=lookupd_http_addresses,
            max_in_flight=max_in_flight,
            loop=loop,
        )
    else:
        if nsqd_tcp_addresses is None:
            nsqd_tcp_addresses = ["127.0.0.1:4150"]
        nsqd_tcp_addresses = [get_host_and_port(i) for i in nsqd_tcp_addresses]
        reader = Reader(
            nsqd_tcp_addresses=nsqd_tcp_addresses,
            max_in_flight=max_in_flight,
            loop=loop,
            **kwargs,
        )
    await reader.connect()
    return reader


class Reader:
    """
    NSQ tcp reader
    """

    def __init__(
        self,
        nsqd_tcp_addresses=None,
        lookupd_http_addresses=None,
        max_in_flight=42,
        loop=None,
        heartbeat_interval=30000,
        feature_negotiation=True,
        tls_v1=False,
        snappy=False,
        deflate=False,
        deflate_level=6,
        sample_rate=0,
        consumer=False,
        user_agent="",
    ):
        user_agents = ["nsqio/{}".format(get_version())]
        if user_agent:
            user_agents.append(user_agent)
        self._config = {
            "user_agent": " ".join(user_agents),
            "deflate": deflate,
            "deflate_level": deflate_level,
            "sample_rate": sample_rate,
            "snappy": snappy,
            "tls_v1": tls_v1,
            "heartbeat_interval": heartbeat_interval,
            "feature_negotiation": feature_negotiation,
        }
        self._nsqd_tcp_addresses = nsqd_tcp_addresses or []
        self._lookupd_http_addresses = lookupd_http_addresses or []

        self._max_in_flight = max_in_flight
        self._loop = loop or asyncio.get_event_loop()
        self._queue = asyncio.Queue(loop=self._loop)

        self._idle_timeout = 10

        self._max_in_flight = max_in_flight
        self._num_readers = 0

        self._is_subscribe = False
        self._redistribute_timeout = 5  # sec
        self._lookupd_poll_time = 30  # sec
        self.topic = None
        self.channel = None
        self._rdy_control = RdyControl(
            idle_timeout=self._idle_timeout,
            max_in_flight=self._max_in_flight,
            loop=self._loop,
        )
        self._auto_poll_lookupd_task_closed = asyncio.Event(loop=self._loop)
        self._auto_poll_lookupd_task = None

    async def connect(self):
        logging.info("reader connecting")
        if self._lookupd_http_addresses:
            """
            since lookupd must find a topic to find nsqds, 
            lookupd connect changed in to subscribe func
            """
            pass
        if self._nsqd_tcp_addresses:
            connections = {}
            for host, port in self._nsqd_tcp_addresses:
                conn: "TcpConnection" = await create_connection(
                    host, port, queue=self._queue, loop=self._loop
                )
                await self.prepare_conn(conn)
                connections[conn.id] = conn
            self._rdy_control.add_connections(connections)
        # init distribute for conns, init update rdy state for conn
        self._rdy_control.redistribute()

    async def prepare_conn(self, conn: "TcpConnection"):
        conn._on_message = partial(self._on_message, conn)
        _ = await conn.identify(**self._config)

    def _on_message(self, conn, msg):
        conn._last_message = time.time()
        if conn._on_rdy_changed_cb is not None:
            conn._on_rdy_changed_cb(conn.id)
        return msg

    async def _poll_lookupd(self, host, port):
        nsqlookup_conn = None
        try:
            nsqlookup_conn = NsqLookupd(host, port, loop=self._loop)

            res = await nsqlookup_conn.lookup(self.topic)
            logger.debug("lookupd response {}".format(res))

            if "producers" in res:
                addrs = []
                for producer in res["producers"]:
                    host = producer["broadcast_address"]
                    port = producer["tcp_port"]
                    addrs.append((host, port))
                return addrs
            else:
                logger.debug("producers not found error")
                return []
        except Exception as e:
            logger.error(e)
            return []
        finally:
            if nsqlookup_conn is not None:
                try:
                    await nsqlookup_conn.close()
                except Exception as e:
                    logger.error(e)

    async def _init_lookupd_conns(self, host, port):
        producers = await self._poll_lookupd(host, port)
        if len(producers) == 0:
            # no producer
            logger.debug("No producer detected")
            return False
        for producer in producers:
            try:
                p_host, p_port = producer
                tmp_id = "tcp://{}:{}".format(p_host, p_port)
                if not await self._rdy_control._is_valid_connection(tmp_id):
                    logger.debug(
                        "new connection: host={}, port={}".format(p_host, p_port)
                    )
                    conn = await create_connection(
                        host=p_host, port=p_port, queue=self._queue, loop=self._loop
                    )
                    await self.prepare_conn(conn)
                    logger.debug("conn.id={}".format(conn.id))
                    # self._rdy_control.connections[conn.id] = conn
                    self._rdy_control.add_connection(conn)
            except Exception as e:
                logger.error(e)
                return False
        return True

    async def _auto_poll_lookupd(self):
        logger.debug("starting _auto_poll_lookupd")
        timeout_generator = retry_iterator(init_delay=1, max_delay=30.0)
        try:
            while (
                self._is_subscribe
                and self.topic is not None
                and self.channel is not None
            ):
                logger.info("reader _auto_poll_lookupd check loop")

                host, port = random.choice(self._lookupd_http_addresses)
                producers = await self._poll_lookupd(host, port)
                if len(producers) > 0:
                    # no producer
                    for producer in producers:
                        conn = None
                        try:
                            p_host, p_port = producer
                            tmp_id = "tcp://{}:{}".format(p_host, p_port)
                            # if tmp_id not in self._rdy_control.connections:
                            # missing? add it!
                            if not await self._rdy_control._is_valid_connection(tmp_id):
                                logger.debug(
                                    "_auto_poll_lookupd: new connection: host={}, port={}".format(
                                        p_host, p_port
                                    )
                                )
                                conn = await create_connection(
                                    host=p_host,
                                    port=p_port,
                                    queue=self._queue,
                                    loop=self._loop,
                                )
                                logger.debug("conn.id={}".format(conn.id))
                                await self.prepare_conn(conn)
                                # self._rdy_control.connections[conn.id] = conn
                                self._rdy_control.add_connection(conn)

                                await self.sub(conn, self.topic, self.channel)

                                logger.debug(
                                    "new nsqd added: conn.id={}".format(conn.id)
                                )

                                # why?
                                if conn._on_rdy_changed_cb is not None:
                                    conn._on_rdy_changed_cb(conn.id)
                            # else:
                            #     logger.debug("skip... {}".format(tmp_id))
                        except Exception as e:
                            logger.error(e)
                            if conn is not None:
                                try:
                                    conn.close()
                                except Exception as e:
                                    logger.error(e)
                        finally:
                            if not self._is_subscribe and conn is not None:
                                try:
                                    conn.close()
                                except Exception as e:
                                    logger.error(e)
                        if not self._is_subscribe:
                            break

                t = next(timeout_generator)
                await asyncio.sleep(t, loop=self._loop)
        except asyncio.CancelledError:
            logger.info("{} _auto_poll_lookupd".format(self))
        finally:
            self._auto_poll_lookupd_task_closed.set()

    async def subscribe(self, topic, channel):
        self.topic = topic
        self.channel = channel
        self._is_subscribe = True

        # firstly, we check lookupd
        if self._lookupd_http_addresses:
            lookupd_status = await self._lookupd()
            if not lookupd_status:
                logger.warning("init lookupd failed! lookupd_http_addresses: {}".format(self._lookupd_http_addresses))
                #TODO(yu): no need to return False for empty initialization
                # return False
            self._auto_poll_lookupd_task = self._loop.create_task(
                self._auto_poll_lookupd()
            )

        # and then, we sub all available topics
        for conn in self._rdy_control.connections.values():
            await self.sub(conn, topic, channel)
            if conn._on_rdy_changed_cb is not None:
                conn._on_rdy_changed_cb(conn.id)

        # redistribute is a task for fail or overload to
        # rebalance tcpconnections
        # consider for further. disable now
        # self._redistribute_task = self._loop.create_task(self._redistribute())
        return True

    async def sub(self, conn, topic, channel):
        await conn.execute(SUB, topic, channel)

    async def set_max_in_flight(self, max_in_flight):
        for conn in self._rdy_control.connections.values():
            await conn.execute(RDY, max_in_flight)

    async def send_cls(self):
        """ CLS 
            Cleanly close your connection (no more messages are sent)
        """
        for conn in self._rdy_control.connections.values():
            await conn.execute(CLS)

    async def messages(self):
        if not self._is_subscribe:
            logger.warning("You must subscribe to the topic first")
            return

        try:
            logger.debug("num readers ++ ")
            self._num_readers += 1
            while self._is_subscribe:
                result = await self._queue.get()
                if result is not None:
                    yield result
                else:
                    logger.debug("got NONE, skip")
            logger.debug("leaving message receiving")
        finally:
            logger.debug("num readers -- ")
            self._num_readers -= 1

    async def _redistribute(self):
        while self._is_subscribe:
            self._rdy_control.redistribute()
            await asyncio.sleep(self._redistribute_timeout, loop=self._loop)

    async def _lookupd(self):
        host, port = random.choice(self._lookupd_http_addresses)
        return await self._init_lookupd_conns(host, port)

    async def rescan_connections(self):
        return await self._lookupd()

    async def unsubscribe(self, timeout=10):
        if not self._is_subscribe:
            logger.warning("You must subscribe to the topic first")
            return
        # logger.debug("unsubscribing {}".format(self))
        # mark as disabled
        await self.set_max_in_flight(0)
        # clear is_subscribed flag
        self._is_subscribe = False

        # self._auto_poll_lookupd_task_closed = asyncio.Event(loop=self._loop)
        # self._auto_poll_lookupd_task = None
        try:
            if self._auto_poll_lookupd_task is not None:
                logger.info("canceling auto_poll_lookupd_task")
                self._auto_poll_lookupd_task.cancel()
                await asyncio.wait_for(
                    self._auto_poll_lookupd_task_closed.wait(),
                    timeout=timeout,
                    loop=self._loop,
                )
        except Exception as e:
            logger.error("cancel failed: {}".format(e))
        finally:
            logger.info("auto_poll_lookupd_task canceled")

        # send None to clear readers
        num_retries = 0
        while self._num_readers > 0 or num_retries > 1000:
            for i in range(self._num_readers):
                self._queue.put_nowait(None)
            num_retries += 1
            await asyncio.sleep(0.05)
        if self._num_readers > 0:
            logger.error(
                "{} retried {} times but still not work...".format(self, num_retries)
            )
        # no subscribers now

        # clear & req rest of the messages
        # mostly it is empty
        try:
            # consume all unfinished elems
            while not self._queue.empty():
                try:
                    result = self._queue.get_nowait()
                    if result is not None:
                        logger.debug("req: {}".format(result))
                        await result.req(0)
                except Exception as e:
                    logger.warning("req message failed {}".format(e))
        except Exception as e:
            logger.warning("requeue all messages failed {}".format(e))

    async def close(self):
        if self._is_subscribe:
            await self.unsubscribe()
        try:
            # await self.send_cls()
            # clear rdy_control
            if self._rdy_control is not None:
                self._rdy_control.stop_working()
            # close all connections
            # TODO: move to _rdy_control later
            for conn in self._rdy_control.connections.values():
                if conn is not None:
                    try:
                        conn.close()
                        await conn.wait_for_closed(1)
                    except Exception as e:
                        logger.error(e)
        except Exception as e:
            logger.error("close failed: {}".format(e))

    def __repr__(self):
        return "<Reader{}/{}>".format(self.topic, self.channel)
