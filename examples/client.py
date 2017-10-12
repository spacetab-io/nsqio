import asyncio
from asyncnsq.consumer import NsqConsumer
from asyncnsq.nsq import create_nsq


def main():

    loop = asyncio.get_event_loop()

    async def go():
        nsq_producer = await create_nsq(host='127.0.0.1', port=4150,
                                             heartbeat_interval=30000,
                                             feature_negotiation=True,
                                             tls_v1=True,
                                             snappy=False,
                                             deflate=False,
                                             deflate_level=0,
                                             loop=loop)
        for i in range(0, 35):
            await nsq_producer.pub('test_asyncnsq', 'xxx:{i}'.format(i=i))

        endpoints = [('127.0.0.1', 4150)]
        nsq_consumer = NsqConsumer(nsqd_tcp_addresses=endpoints, loop=loop)
        await nsq_consumer.connect()
        await nsq_consumer.subscribe('test_asyncnsq', 'nsq')
        for waiter in nsq_consumer.wait_messages():
            message = await waiter
            print(message.body)
            await message.fin()

    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
