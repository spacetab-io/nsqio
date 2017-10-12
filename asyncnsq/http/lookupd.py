import asyncio
from .base import NsqHTTPConnection


class NsqLookupd(NsqHTTPConnection):
    """
    :see: http://nsq.io/components/nsqlookupd.html
    """

    async def ping(self):
        """Monitoring endpoint.
        :returns: should return `"OK"`, otherwise raises an exception.
        """
        return self.perform_request('GET', 'ping', None, None)

    async def info(self):
        """Returns version information."""
        response = await self.perform_request('GET', 'info', None, None)
        return response

    async def lookup(self, topic):
        """XXX

        :param topic:
        :return:
        """
        response = await self.perform_request(
            'GET', 'lookup', {'topic': topic}, None)
        return response

    async def topics(self):
        """XXX

        :return:
        """
        resp = await self.perform_request('GET', 'topics', None, None)
        return resp

    async def channels(self, topic):
        """XXX

        :param topic:
        :return:
        """
        resp = await self.perform_request(
            'GET', 'channels', {'topic': topic}, None)
        return resp

    async def nodes(self):
        """XXX

        :return:
        """
        resp = await self.perform_request('GET', 'nodes', None, None)
        return resp

    async def delete_topic(self, topic):
        """XXX

        :param topic:
        :return:
        """
        resp = await self.perform_request(
            'GET', 'delete_topic', {'topic': topic}, None)
        return resp

    async def delete_channel(self, topic, channel):
        """XXX

        :param topic:
        :param channel:
        :return:
        """
        resp = await self.perform_request(
            'GET', 'delete_channel', {'topic': topic, 'channel': channel},
            None)
        return resp

    async def tombstone_topic_producer(self, topic, node):
        """XXX

        :param topic:
        :param node:
        :return:
        """
        resp = await self.perform_request(
            'GET', 'delete_channel', {'topic': topic, 'node': node},
            None)
        return resp
