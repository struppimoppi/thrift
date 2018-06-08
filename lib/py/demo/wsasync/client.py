# -*- coding: utf-8 -*-
import logging
logging.basicConfig(format="%(asctime)s  %(levelname)7s  %(name)20s   %(message)s", level=logging.DEBUG)
import asyncio
from autobahn.asyncio.websocket import WebSocketClientFactory

from thrift.wsasync import on_task_done
from thrift.transport.TWebSocketAsync import TBufferedWebSocketClientTransport
from thrift.protocol.wsasync.tproto import TBinaryMessageTypeDispatchingAcceleratedProtocol
from thrift.protocol.wsasync.wsproto import WSClientProtocol
from thrift.server.TWebSocketAsync import WebSocketConnectionRunner

from comm.server import Server
from comm.client import Client


class ClientHandler:

    async def notify(self, text):
        print("server notified us with: %s" % text)
        return "notified: " + text


async def run_app(loop):
    factory = WebSocketClientFactory("ws://127.0.0.1:9000")
    factory.protocol = WSClientProtocol
    websocket_proto = factory()

    transport = TBufferedWebSocketClientTransport(websocket_proto, loop)
    protocol = TBinaryMessageTypeDispatchingAcceleratedProtocol(transport)
    processor = Client.Processor(ClientHandler())

    await transport.open()
    await websocket_proto.wait_for_opened()  # raises asyncio.TimeoutError

    # RPCs an uns, also Requetss an unseren Service
    conn_runner = WebSocketConnectionRunner(transport, protocol, processor)
    conn_runner.start_async_task()

    # den Service des Servers benutzen
    server = Server.Client(protocol)

    for i in range(1):  # @UnusedVariable
        response = await server.work("some work")
        print('server responded %s' % response)
        await asyncio.sleep(1)

    await asyncio.wait([conn_runner.task])
    transport.close()


def run_app_done(ft):
    on_task_done("run_websocket_connection", ft)
    loop.stop()


if __name__ == '__main__':

    loop = asyncio.get_event_loop()
    asyncio.ensure_future(run_app(loop)).add_done_callback(run_app_done)
    loop.run_forever()
    loop.close()
