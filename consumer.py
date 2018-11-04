import asyncio
import sys
import os
import logging
sys.path.append(os.getcwd())
print(sys.path)
from asyncnsq.tcp.reader import create_reader

logger = logging.getLogger()


def main():

    loop = asyncio.get_event_loop()

    async def go():
        try:
            # nsq_consumer = await create_reader(
            #     lookupd_http_addresses=[
            #         ('127.0.0.1', 4161)],
            #     max_in_flight=200)
            # await nsq_consumer.subscribe('test_async_nsq', 'nsq')
            # async for message in nsq_consumer.messages():
            #     print(message.body)
            #     await message.fin()
            nsq_consumer = await create_reader(
                nsqd_tcp_addresses=['127.0.0.1:4150'],
                max_in_flight=200)
            await nsq_consumer.subscribe('test_async_nsq', 'nsq')
            async for message in nsq_consumer.messages():
                print(message.body)
                await message.fin()
        except Exception as tmp:
            logger.exception(tmp)

    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
