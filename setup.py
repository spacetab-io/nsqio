import re
import os.path
import sys
from setuptools import setup, find_packages


install_requires = ['python-snappy']

PY_VER = sys.version_info

if PY_VER >= (3, 4):
    pass
elif PY_VER >= (3, 3):
    install_requires.append('asyncio')
else:
    raise RuntimeError("asyncnsq doesn't support Python version prior 3.3")


def read(*parts):
    with open(os.path.join(*parts), 'rt') as f:
        return f.read().strip()


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*'([\d.abrc]+)'")
    init_py = os.path.join(os.path.dirname(__file__),
                           'asyncnsq', '__init__.py')
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        else:
            raise RuntimeError('Cannot find version in asyncnsq/__init__.py')


classifiers = [
    'License :: OSI Approved :: MIT License',
    'Development Status :: 4 - Beta',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Operating System :: POSIX',
    'Environment :: Web Environment',
    'Intended Audience :: Developers',
    'Topic :: Software Development',
    'Topic :: Software Development :: Libraries',
]

long_description = "asyncnsq\n========\n\nasync nsq with python3.6 await/async supported\n\n**if you dont like the pynsq(which use tornado) way to interact with\nnsq, then this library may be suitable for you**\n\nyou can use this library as the common way to write things\n\nLatest Updates\n--------------\n\n-  support dpub\n-  support lookupd_http\n\nInstall\n-------\n\npip install asyncnsq\n\nUsage examples\n--------------\n\nAll you need is a loop, then enjoy\n\nConsumer:\n\n::\n\n    loop = asyncio.get_event_loop()\n\n    async def go():\n        try:\n            nsq_consumer = await create_nsq_consumer(\n                lookupd_http_addresses=[\n                    ('127.0.0.1', 4161)],\n                max_in_flight=200)\n            await nsq_consumer.subscribe('test_async_nsq', 'nsq')\n            for waiter in nsq_consumer.wait_messages():\n                message = await waiter\n                print(message.body)\n                await message.fin()\n            nsq_consumer = await create_nsq_consumer(\n                host=['tcp://127.0.0.1:4150'],\n                max_in_flight=200)\n            await nsq_consumer.subscribe('test_async_nsq', 'nsq')\n            for waiter in nsq_consumer.wait_messages():\n                message = await waiter\n                print(message.body)\n                await message.fin()\n        except Exception as tmp:\n            logger.exception(tmp)\n\n    loop.run_until_complete(go())\n\nProducer:\n\n::\n\n    loop = asyncio.get_event_loop()\n\n    async def go():\n        nsq_producer = await create_nsq_producer(host='127.0.0.1', port=4150,\n                                                 heartbeat_interval=30000,\n                                                 feature_negotiation=True,\n                                                 tls_v1=True,\n                                                 snappy=False,\n                                                 deflate=False,\n                                                 deflate_level=0,\n                                                 loop=loop)\n        for i in range(10):\n            await nsq_producer.pub('test_async_nsq', 'test_async_nsq:{i}'.format(i=i))\n            await nsq_producer.dpub('test_async_nsq', i * 1000,\n                                    'test_delay_async_nsq:{i}'.format(i=i))\n    loop.run_until_complete(go())\n\nRequirements\n------------\n\n-  Python\\_ 3.5+ https://www.python.org\n-  nsq\\_ http://nsq.io\n\n-  python-snappy\n\n   1. ubuntu:\n\n      -  sudo apt-get install libsnappy-dev\n      -  pip install python-snappy\n\n   2. centos:\n\n      -  sudo yum install snappy-devel\n      -  pip install python-snappy\n\n   3. mac:\n\n      -  brew install snappy # snappy library from Google\n      -  CPPFLAGS=“-I/usr/local/include -L/usr/local/lib” pip install\n         python-snappy\n\nLicense\n-------\n\nThe asyncnsq is offered under MIT license.\n"

setup(name='asyncnsq',
      version=read_version(),
      description=("asyncio async/await nsq support"),
      long_description=long_description,
      classifiers=classifiers,
      platforms=["POSIX"],
      author="aohan237",
      author_email="aohan237@gmail.com",
      url="https://github.com/aohan237/asyncnsq",
      license="MIT",
      packages=find_packages(exclude=["tests"]),
      install_requires=install_requires,
      include_package_data=True,
      )
