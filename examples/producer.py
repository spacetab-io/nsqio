import asyncio
import sys
import os
sys.path.append(os.getcwd())
from asyncnsq import create_nsq_writer


def main():

    loop = asyncio.get_event_loop()

    async def go():
        nsq_producer = await create_nsq_writer(host='127.0.0.1', port=4150,
                                               heartbeat_interval=30000,
                                               feature_negotiation=True,
                                               tls_v1=True,
                                               snappy=False,
                                               deflate=False,
                                               deflate_level=0,
                                               loop=loop)
        for i in range(100):
            await nsq_producer.pub('test_async_nsq', 'test_async_nsq:{i}'.format(i=i))
            await nsq_producer.dpub('test_async_nsq', i * 1000,
                                    'test_delay_async_nsq:{i}'.format(i=i))

    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
