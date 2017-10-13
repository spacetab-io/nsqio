asyncnsq (work in progress,仍然开发中)
=========================

implement python3.5+  async/await syntax support

提供python3.5+  async/await 语法支持。 原项目，支持yield from 生成器语法。

this project is forked from  jettify/aionsq, he is the original author

本项目fork了 jettify/aionsq， 他是原作者，但是很久没有更新了。

Usage examples
--------------
you can refer from examples.

你可以查看examples文件夹中的例子。


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
