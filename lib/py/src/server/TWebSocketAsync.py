# -*- coding: utf-8 -*-
import asyncio
import logging
from thrift.Thrift import TMessageType

from thrift.wsasync import on_task_done


class WebSocketConnectionRunner:

    def __init__(self, ttrans, tproto, processor):
        self.ttransport = ttrans
        self.tprotocol = tproto
        self.processor = processor

        self.task = None

    async def run(self):

        while 1:
            try:
                iproto = await self.tprotocol.wait_for_message(TMessageType.CALL, TMessageType.ONEWAY)

                await self.processor.process(iproto, self.tprotocol)

            except asyncio.CancelledError:
                break
            except Exception as x:
                logging.exception(x)
                break

        self.ttransport.close()

    def start_async_task(self):
        self.task = asyncio.ensure_future(self.run())
        self.task.add_done_callback(lambda ft: on_task_done("WebSocketConnectionRunner.run()", ft))


class ClientRegistry:
    """
    Auf Serverseite zum tracken der verbundenen Clients
    """

    def __init__(self, ClientStub):
        self._ClientStub = ClientStub
        self._clients = {}

    def new_connection(self, tprotocol, peer):
        client = self._ClientStub(tprotocol)
        client.peer = peer
        self._clients[peer] = client

    def drop_connection(self, peer):
        del self._clients[peer]

    def __iter__(self):
        return iter(self._clients.values())
