import asyncio
from asyncio.streams import StreamWriter, StreamReader
import json
import ssl
import logging

from collections import deque

from nsqio.tcp.consts import (
    MAGIC_V2,
    BIN_OK,
    HEARTBEAT,
    FRAME_TYPE_RESPONSE,
    FRAME_TYPE_ERROR,
    FRAME_TYPE_MESSAGE,
)

from nsqio.tcp.messages import NsqMessage
from nsqio.tcp.exceptions import ProtocolError  # , make_error
from nsqio.tcp.protocol import Reader, DeflateReader, SnappyReader

logger = logging.getLogger(__name__)


async def create_connection(
    host: str = "localhost", port: int = 4150, queue=None, loop=None
):
    """create nsq tcp connection
    Args:
        host: host address
        port: host port
        queue: user define asyncio queue
        loop: user define asyncio loop
    Return:
        TcpConnection
    """
    reader, writer = await asyncio.open_connection(host, port, loop=loop)
    conn = TcpConnection(reader, writer, host, port, queue=queue, loop=loop)
    conn.connect()
    return conn


class TcpConnection:
    """
    base nsq connection class ,used for manipulate reader/writer content
    """

    def __init__(
        self,
        reader: StreamReader,
        writer: StreamWriter,
        host: str,
        port: int,
        *,
        on_rdy_changed=None,
        on_message=None,
        queue=None,
        loop=None,
    ):
        self._reader, self._writer = reader, writer
        self._raw_writer_transport = self._writer.transport
        self._host, self._port = host, port

        self._loop = loop or asyncio.get_event_loop()

        assert isinstance(queue, asyncio.Queue) or queue is None
        self._queue = queue or asyncio.Queue(loop=self._loop)

        self._parser = Reader()
        # next queue is used for nsq commands
        self._cmd_waiters = deque()
        self._closing = False
        self._closed = False
        self._reader_task = self._loop.create_task(self._read_data())
        # mark connection in upgrading state to ssl socket
        self._is_upgrading = False
        self._on_message = on_message
        self._on_rdy_changed_cb = on_rdy_changed
        self._on_close = None
        self._on_close_flag = asyncio.Event(loop=self._loop)

        # number of received but not acked or req messages
        self._in_flight = 0
        logger.info("new connection: {}:{}".format(self._host, self._port))

    def connect(self):
        self._send_magic()
        logger.info("connect: {}:{}".format(self._host, self._port))

    def execute(self, command: bytes, *args, data=None, cb=None):
        """XXX"""
        assert (
            self._reader and not self._reader.at_eof()
        ), "Connection closed or corrupted"
        if command is None:
            raise TypeError("command must not be None")
        if None in set(args):
            raise TypeError("args must not contain None")
        fut = asyncio.Future(loop=self._loop)

        if command in (b"NOP", b"FIN", b"RDY", b"REQ", b"TOUCH"):
            fut.set_result(b"OK")
        else:
            self._cmd_waiters.append((fut, cb))
        command_raw = self._parser.encode_command(command, *args, data=data)
        logger.debug("execute command {}".format(command_raw))
        self._writer.write(command_raw)

        # track all processed and requeued messages
        if command in (b"FIN", b"REQ", "FIN", "REQ"):
            self._in_flight = max(0, self._in_flight - 1)
        return fut

    @property
    def in_flight(self):
        return self._in_flight

    @property
    def endpoint(self):
        return "tcp://{}:{}".format(self._host, self._port)

    @property
    def id(self):
        return self.endpoint

    @property
    def closed(self):
        """True if connection is closed."""
        closed = self._closing or self._closed
        conn_lost = False
        if self._raw_writer_transport is None:
            logger.warning("_raw_writer_transport is None!!")
            conn_lost = True
        elif (
            self._raw_writer_transport._conn_lost
        ):  # it may indices some error for the value > 0
            logger.warning("_raw_writer_transport._conn_lost detected")
            conn_lost = True
        elif self._writer is None:
            logger.warning("_writer is None!!")
            conn_lost = True
        elif self._writer.transport._conn_lost:
            logger.warning("_writer.transport._conn_lost detected")
            conn_lost = True

        if conn_lost:
            logger.warning("conn is LOST!!!")
        else:
            logger.debug(
                "{} i am fine! {} ".format(self, self._raw_writer_transport._conn_lost)
            )
        if not closed and (self._reader and self._reader.at_eof() or conn_lost):
            self._closing = closed = True
            self._loop.call_soon(self._do_close, None)
        return closed

    async def wait_for_closed(self, timeout=10):
        await asyncio.wait_for(self._on_close_flag.wait(), timeout, loop=self._loop)

    @property
    def queue(self):
        return self._queue

    def close(self):
        """Close connection."""
        logger.debug("closing {}".format(self.id))
        self._do_close()

    async def identify(self, **config):
        # TODO: add config validator
        data = json.dumps(config)
        resp = await self.execute(b"IDENTIFY", data=data, cb=self._start_upgrading)
        if resp in (b"OK", "OK"):
            self._finish_upgrading()
            return resp
        resp_config = json.loads(resp.decode("utf-8"))
        fut = None
        if resp_config.get("tls_v1"):
            await self._upgrade_to_tls()

        if resp_config.get("snappy"):
            fut = self._upgrade_to_snappy()
        elif resp_config.get("deflate"):
            fut = self._upgrade_to_deflate()
        self._finish_upgrading()
        if fut:
            ok = await fut
            assert ok == b"OK"
        return resp

    def _do_close(self, exc=None):
        logger.info("this is the going close info")
        if exc:
            logger.error("Connection closed with error: {}".format(exc))
        if self._closed:
            return
        self._closed = True
        self._closing = False
        self._writer.transport.close()
        self._reader_task.cancel()
        self._on_close_flag.set()

    def _send_magic(self):
        self._writer.write(MAGIC_V2)

    def drain(self):
        self._loop.create_task(self._drain())

    async def _drain(self):
        try:
            # logger.warning("drain....")
            await self._writer.drain()
            logger.debug("drained.... OK")
        except Exception as e:
            logger.warning("{} drain writer failed!!! {}".format(self, e))
            self._do_close(e)

    def _pulse(self):
        # logger.info("_pulse")
        nop = self._parser.encode_command(b"NOP")
        self._writer.write(nop)
        self._loop.create_task(self._drain())

    async def _upgrade_to_tls(self):
        self._reader_task.cancel()
        transport = self._writer.transport
        transport.pause_reading()
        raw_sock = transport.get_extra_info("socket", default=None)
        if raw_sock is None:
            raise RuntimeError("Transport does not expose socket instance")
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)

        self._reader, self._writer = await asyncio.open_connection(
            sock=raw_sock, ssl=ssl_context, loop=self._loop, server_hostname=self._host
        )
        bin_ok = await self._reader.readexactly(10)
        if bin_ok != BIN_OK:
            raise RuntimeError("Upgrade to TLS failed, got: {}".format(bin_ok))
        self._reader_task = self._loop.create_task(self._read_data())
        self._reader_task.add_done_callback(self._on_reader_task_stopped)

    def _on_reader_task_stopped(self, future):
        exc = future.exception()
        logger.error("DONE: TASK {}".format(exc))

    def _upgrade_to_snappy(self):
        self._parser = SnappyReader(self._parser.buffer)
        fut = asyncio.Future(loop=self._loop)
        self._cmd_waiters.append((fut, None))
        return fut

    def _upgrade_to_deflate(self):
        self._parser = DeflateReader(self._parser.buffer)
        fut = asyncio.Future(loop=self._loop)
        self._cmd_waiters.append((fut, None))
        return fut

    async def _read_data(self):
        """Response reader task."""
        is_canceled = False
        # logger.debug("{} starting _read_data".format(self))
        while not self._reader.at_eof():
            try:
                data = await self._reader.read(52)
            except asyncio.CancelledError:
                is_canceled = True
                logger.debug("Task is canceled {}".format(self))
                break
            except Exception as exc:
                logger.exception(exc)
                logger.debug("Reader task stopped due to: {}".format(exc))
                break
            self._parser.feed(data)
            not self._is_upgrading and self._read_buffer()

        if is_canceled:
            # useful during update to TLS, task canceled but connection
            # should not be closed
            return
        logger.info("{} is read to end, going to close".format(self.id))
        self._closing = True
        self._loop.call_soon(self._do_close, None)

    def _parse_data(self):
        try:
            obj = self._parser.gets()
        except ProtocolError as exc:
            # ProtocolError is fatal
            # so connection must be closed
            logger.exception(exc)
            self._closing = True
            self._loop.call_soon(self._do_close, exc)
            logger.error("ProtocolError is fatal")
            return
        else:
            if obj is False:
                return False
            logger.debug("got nsq data: %s", obj)
            resp_type, resp = obj
            hb = HEARTBEAT
            # print(resp_type, resp)
            if resp_type == FRAME_TYPE_RESPONSE and resp == hb:
                self._pulse()
            elif resp_type == FRAME_TYPE_RESPONSE:
                waiter, cb = self._cmd_waiters.popleft()
                if not waiter.cancelled():
                    waiter.set_result(resp)
                    cb is not None and cb(resp)
            elif resp_type == FRAME_TYPE_ERROR:
                waiter, cb = self._cmd_waiters.popleft()
                # error = make_error(*resp)
                if not waiter.cancelled():
                    waiter.set_result(resp)
                    cb is not None and cb(resp)
            elif resp_type == FRAME_TYPE_MESSAGE:

                # track number in flight messages
                self._in_flight += 1

                ts, att, msg_id, body = resp
                self._on_message_hook(ts, att, msg_id, body)
                # self._queue.put_nowait(msg)
            return True

    def _on_message_hook(self, ts, att, msg_id, body):
        msg = NsqMessage(ts, att, msg_id, body, self)
        if self._on_message:
            msg = self._on_message(msg)
        self._queue.put_nowait(msg)

    def _read_buffer(self):
        is_continue = True
        while is_continue:
            is_continue = self._parse_data()

    def _start_upgrading(self, resp=None):
        self._is_upgrading = True

    def _finish_upgrading(self, resp=None):
        self._read_buffer()
        self._is_upgrading = False

    def __repr__(self):
        return "<TcpConnection: {}:{} ~{}>".format(
            self._host, self._port, self._in_flight
        )
