import asyncio
import logging
from nsqio import create_reader


logger = logging.getLogger(__name__)


def main():

    loop = asyncio.get_event_loop()

    async def go():
        try:
            reader = await create_reader(
                lookupd_http_addresses=[("127.0.0.1", 4161)], max_in_flight=200
            )
            await reader.subscribe("test_async_nsq", "nsq")
            async for message in reader.messages():
                print(message.body)
                await message.fin()
        except Exception as tmp:
            logger.exception(tmp)

    loop.run_until_complete(go())


def tcp_main():

    loop = asyncio.get_event_loop()

    async def go():
        try:
            reader = await create_reader(
                nsqd_tcp_addresses=["127.0.0.1:4150"], max_in_flight=200
            )
            await reader.subscribe("test_async_nsq", "nsq")
            async for message in reader.messages():
                print(message.body)
                await message.fin()
        except Exception as tmp:
            logger.exception(tmp)

    loop.run_until_complete(go())


if __name__ == "__main__":
    # main()
    tcp_main()
