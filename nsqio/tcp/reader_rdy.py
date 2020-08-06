from nsqio.tcp.connection import logger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nsqio.tcp.connection import TcpConnection
    from typing import Dict

import asyncio
import random
import traceback
from nsqio.tcp.consts import RDY

REDISTRIBUTE = 0
CHANGE_CONN_RDY = 1
NOOP = 2


class RdyControl:
    def __init__(self, idle_timeout, max_in_flight, loop=None):
        self._connections: "Dict[str, TcpConnection]" = {}
        self._idle_timeout = idle_timeout
        self._total_ready_count = 0
        self._max_in_flight = max_in_flight
        self._loop = loop or asyncio.get_event_loop()

        self._cmd_queue = asyncio.Queue(loop=self._loop)

        self._expected_rdy_state = {}

        self._is_working = True

        self._distributor_task = self._loop.create_task(self._distributor())

    @property
    def connections(self):
        return self._connections

    def add_connections(self, connections):
        prev_connections = self._connections
        self._connections = connections
        for conn in self._connections.values():
            conn._on_rdy_changed_cb = self.rdy_changed

        self._close_all_connections(prev_connections)

    def add_connection(self, connection):
        connection._on_rdy_changed_cb = self.rdy_changed
        id = connection.id
        if id in self._connections:
            self._connections[id].close()
        self._connections[connection.id] = connection

    def rdy_changed(self, conn_id):
        self._cmd_queue.put_nowait((CHANGE_CONN_RDY, (conn_id,)))

    def redistribute(self):
        self._cmd_queue.put_nowait((REDISTRIBUTE, ()))

    async def _distributor(self):
        while self._is_working:
            cmd, args = await self._cmd_queue.get()
            if cmd == REDISTRIBUTE:
                await self._redistribute_rdy_state()
            elif cmd == CHANGE_CONN_RDY:
                await self._update_rdy(*args)
            elif cmd == NOOP:
                continue
            else:
                RuntimeError("Should never be here")

    def remove_connection(self, conn: "TcpConnection"):
        self._connections.pop(conn.id)
        conn.close()

    def remove_all(self):
        prev_connections = self._connections

        self._connections = {}

        self._close_all_connections(prev_connections)

    def _close_all_connections(self, connections):
        for conn in connections.values():
            if not conn.closed:
                conn.close()

    def stop_working(self):
        self._is_working = False
        self._cmd_queue.put_nowait((NOOP, ()))
        self.remove_all()

    async def _redistribute_rdy_state(self):
        # We redistribute RDY counts in a few cases:
        #
        # 1. our # of connections exceeds our configured max_in_flight
        # 2. we're in backoff mode (but not in a current backoff block)
        # 3. something out-of-band has set the need_rdy_redistributed flag
        # (connection closed
        # that was about to get RDY during backoff)
        #
        # At a high level, we're trying to mitigate stalls related to
        # -volume
        # producers when we're unable (by configuration or backoff) to provide
        # a RDY count
        # of (at least) 1 to all of our connections.

        # check all connections
        for conn_id in set(self._connections.keys()):
            await self._is_valid_connection(conn_id)

        if len(self._connections.keys()) == 0:
            logger.warning("no valid connectionss.., skip...")
            return

        connections = self._connections.values()
        # disable for further deprecate

        # rdy_coros = [
        #     conn.execute(RDY, 0) for conn in connections
        #     if not (conn.rdy_state == 0 or
        #             (time.time() - conn.last_message) < self._idle_timeout)
        # ]

        distributed_rdy = sum(c._in_flight for c in connections)
        not_distributed_rdy = self._max_in_flight - distributed_rdy

        random_connections = random.sample(
            list(connections), min(not_distributed_rdy, len(connections))
        )

        rdy_coros = [conn.execute(RDY, 1) for conn in random_connections]

        await asyncio.gather(*rdy_coros)

    async def _is_valid_connection(self, conn_id):
        conn = self._connections.get(conn_id, None)
        if not conn:
            logger.debug("connection {} not found, skip...".format(conn_id))
            return False

        if conn.closed:
            try:
                logger.warning("connection {} closed, removing..".format(conn))
                await self.remove_connection(conn)
            except Exception as e:
                logger.error(
                    "remove_connectionfailed: {}\n{}".format(e, traceback.format_exc())
                )
            finally:
                return False
        return True

    async def _update_rdy(self, conn_id):
        logger.debug("_update_rdy :{}".format(conn_id))
        if not await self._is_valid_connection(conn_id):
            return

        conn = self._connections[conn_id]

        # this is the configuration max_in_flight split even on conn
        base_conn_max_in_flight = self._max_in_flight / max(1, len(self._connections))

        # this is the in_flight number of the conn_id's conn
        conn_in_flight = conn._in_flight

        # get the max rdy state for conn
        rdy_state = int(max(1, base_conn_max_in_flight - conn_in_flight))
        await conn.execute(RDY, rdy_state)
