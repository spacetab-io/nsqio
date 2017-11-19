asyncnsq (work in progress,仍然开发中)
=========================
asyncnsq with python3.6 await/async supported

Latest Updates
--------------

support dpub 


Install
-------------

pip install asyncnsq

Usage examples
--------------

All you need is a loop, then enjoy

Consumer:

    loop = asyncio.get_event_loop()

    async def go():
        nsq_consumer = await create_nsq_consumer(host='tcp://127.0.0.1:4150',
                                                 max_in_flight=200)
        await nsq_consumer.subscribe('test_async_nsq', 'nsq')
        for waiter in nsq_consumer.wait_messages():
            message = await waiter
            print(message.body)
            await message.fin()

    loop.run_until_complete(go())

Producer:

    loop = asyncio.get_event_loop()

    async def go():
        nsq_producer = await create_nsq_producer(host='127.0.0.1', port=4150,
                                                 heartbeat_interval=30000,
                                                 feature_negotiation=True,
                                                 tls_v1=True,
                                                 snappy=False,
                                                 deflate=False,
                                                 deflate_level=0,
                                                 loop=loop)
        for i in range(10):
            await nsq_producer.pub('test_async_nsq', 'test_async_nsq:{i}'.format(i=i))
            await nsq_producer.dpub('test_async_nsq', i * 1000,
                                    'test_delay_async_nsq:{i}'.format(i=i))
    loop.run_until_complete(go())

you can use host like 'tcp://127.0.0.1:4150' or '127.0.0.1' with port=4150

As for now, only single nsqd addr supported.

Requirements
------------

* Python_ 3.5+  https://www.python.org
* nsq_  http://nsq.io

* python-snappy
    1. ubuntu:
        - sudo apt-get install libsnappy-dev
        - pip install python-snappy
    2. centos:
        - sudo yum install snappy-devel
        - pip install python-snappy
    3. mac:
        - brew install snappy # snappy library from Google
        - CPPFLAGS="-I/usr/local/include -L/usr/local/lib" pip install python-snappy

License
-------

The asyncnsq is offered under MIT license.
