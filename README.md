# asyncnsq
[![Downloads](https://pepy.tech/badge/asyncnsq)](https://pepy.tech/project/asyncnsq)
[![PyPI version](https://badge.fury.io/py/asyncnsq.svg)](https://badge.fury.io/py/asyncnsq)

## async nsq with python3.6 await/async supported

**if you dont like the pynsq(which use tornado) way to interact with nsq, then this library may be suitable for you**

you can use this library as the common way to write things

## Important

* #### from version 1.0.0 asyncnsq  has a break change in api

* #### it is not stable yet

* #### you may want to use stable " pip install asyncnsq==0.4.5"

## Features

--------------

### Http Client

* support all the method nsq http supplied

### Tcp Client

#### Connection

* low level connection.

#### Reader

* reader from both lookupd for auto finding nsqd

* list of known nsqd but they can not use together.

* above two can't use together

#### Writer

* all the common method for nsqd writer

## Install

--------------

pip install asyncnsq

## Usage examples

--------------

All you need is a loop, then enjoy. you can refer to examples, as well.

Consumer:

```python
from asyncnsq import create_reader
from asyncnsq.utils import get_logger

loop = asyncio.get_event_loop()
async def go():
    try:
        reader = await create_reader(
            nsqd_tcp_addresses=['127.0.0.1:4150'],
            max_in_flight=200)
        await reader.subscribe('test_async_nsq', 'nsq')
        async for message in reader.messages():
            print(message.body)
            await message.fin()
    except Exception as tmp:
        self.logger.exception(tmp)
loop.run_until_complete(go())
```

Producer:
```python
from asyncnsq import create_writer
loop = asyncio.get_event_loop()
async def go():
    writer = await create_writer(host='127.0.0.1', port=4150,
                                       heartbeat_interval=30000,
                                       feature_negotiation=True,
                                       tls_v1=True,
                                       snappy=False,
                                       deflate=False,
                                       deflate_level=0,
                                       loop=loop)
    for i in range(100):
        await writer.pub('test_async_nsq', 'test_async_nsq:{i}'.format(i=i))
        await writer.dpub('test_async_nsq', i * 1000,
                                'test_delay_async_nsq:{i}'.format(i=i))
loop.run_until_complete(go())
```

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
