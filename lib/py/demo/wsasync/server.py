# -*- coding: utf-8 -*-
import asyncio
import logging
logging.basicConfig(format="%(asctime)s  %(levelname)7s  %(name)20s   %(message)s", level=logging.DEBUG)
from autobahn.asyncio.websocket import WebSocketServerFactory
import random

from thrift.protocol.wsasync.wsproto import WSServerProtocol
from thrift.wsasync import on_task_done
from thrift.server.TWebSocketAsync import ClientRegistry

from comm.client import Client
from comm.server import Server


class ServerHandler:

    async def work(self, text):
        print("got work: %s" % text)

        # string shuffle
        ret = [''] * len(text)
        for i in range(len(text)):
            ret[i] = random.choice(text)
        return ''.join(ret)


async def run_app(loop):
    client_registry = ClientRegistry(Client.Client)
    factory = WebSocketServerFactory("ws://127.0.0.1:9000")
    factory.protocol = WSServerProtocol

    def create_new_wsproto():
        """
        für jede neue Websocket-Verbindung wird ein neues WSServerProtocol erstellt
        Wir wrappen das hier, um dem erstellten Objekt noch den ServerProcessor mitzugeben als auch
        einen "Zugriffspunkt" zu haben, um die verbundenen Clients mitzutracken.
        """
        websocket_proto = factory()
        websocket_proto.processor = Server.Processor(ServerHandler())
        websocket_proto.client_registry = client_registry
        return websocket_proto

    # RPCs an uns, also Requetss an unseren Service beantwortet der Server
    await loop.create_server(create_new_wsproto, '127.0.0.1', 9000)
    logging.info("server started...")

    # immer mal die Clients updaten
    while 1:
        await asyncio.sleep(2)

        for client in client_registry:
            print("sending update to %s" % client.peer)
            response = await client.notify("some update")
            print('client responded %s' % response)


def run_app_done(ft):
    on_task_done("run_websocket_connection", ft)
    loop.stop()


if __name__ == '__main__':

    loop = asyncio.get_event_loop()
    asyncio.ensure_future(run_app(loop)).add_done_callback(run_app_done)
    loop.run_forever()
    loop.close()
