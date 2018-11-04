import asyncio
import json
from asyncnsq.tcp.connection import create_connection
from asyncnsq.tcp.consts import PUB


def main():

    loop = asyncio.get_event_loop()

    async def go():
        conn = await create_connection(host='localhost',
                                       port=4151,
                                       queue=None,
                                       loop=None)
        data = json.dumps({'name': 'test'})
        topic = 'test'
        await conn.execute(PUB, topic, data=data)
    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
