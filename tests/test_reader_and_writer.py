import asyncio
from ._testutils import run_until_complete, BaseTest
from asyncnsq.tcp.reader import create_reader
from asyncnsq.tcp.writer import create_writer


class NsqTest(BaseTest):

    def setUp(self):
        self.topic = 'foo'
        self.host = '127.0.0.1'
        self.port = 4150
        super().setUp()

    @run_until_complete
    async def test_writer(self):
        nsq = await create_writer(host=self.host, port=self.port,
                                  heartbeat_interval=30000,
                                  feature_negotiation=True,
                                  tls_v1=True,
                                  snappy=False,
                                  deflate=False,
                                  deflate_level=0,
                                  loop=self.loop)
        for i in range(10):
            pub_res = await nsq.pub('foo', 'bar')
            self.assertEqual(pub_res, b"OK")

    @run_until_complete
    async def test_reader(self):
        nsq = await create_reader(nsqd_tcp_addresses=[
            f"{self.host}:{self.port}"],
            heartbeat_interval=30000,
            feature_negotiation=True,
            tls_v1=True,
            snappy=False,
            deflate=False,
            deflate_level=0,
            loop=self.loop)
        await nsq.subscribe('foo', 'bar')
        num = 0
        async for msg in nsq.messages():
            num += 1
            fin_res = await msg.fin()
            self.assertEqual(fin_res, b"OK")
            if num > 10:
                break
