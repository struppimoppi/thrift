# -*- coding: utf-8 -*-
import asyncio
import logging
from autobahn.asyncio.websocket import WebSocketClientProtocol, WebSocketServerProtocol

from thrift.transport.TWebSocketAsync import TBufferedWebSocketServerTransport
from thrift.protocol.wsasync.tproto import TBinaryMessageTypeDispatchingAcceleratedProtocol
from thrift.server.TWebSocketAsync import WebSocketConnectionRunner

log = logging.getLogger("thrift.wsasync.wsproto")
log.setLevel(logging.WARNING)


class WSProtocolBase:

    OPEN_TIMEOUT = 10

    def __init__(self):
        super().__init__()

        self._open_event = asyncio.Event()
        self._peer = None

        self._on_message_callback = None

    def set_on_message_callback(self, cb):
        self._on_message_callback = cb

    @property
    def is_open(self):
        return self._open_event.is_set()

    async def wait_for_opened(self):
        await asyncio.wait_for(self._open_event.wait(), self.OPEN_TIMEOUT)

    def onConnect(self, response):
        log.debug("(peer=%s) websocket connected" % response.peer)
        self._peer = response.peer

    def onOpen(self):
        log.debug("(peer=%s) websocket connection now open" % self._peer)
        self._open_event.set()

    def onClose(self, wasClean, code, reason):
        log.debug("(peer=%s) websocket connection closed by peer (clean=%s, code=%s, reason=%s)" % \
                      (self._peer, wasClean, code, reason))
        self._open_event.clear()

    def onMessage(self, payload, isBinary):  # @UnusedVariable
        """
        ob `isBinary` True oder False ist, payload ist immer ein bytes-Objekt. Diese Information ist also irrelevant.
        """
        log.debug("(peer=%s) --> recieved %d bytes" % (self._peer, len(payload)))
        if self._on_message_callback:
            self._on_message_callback(payload)

    def sendMessage(self, payload, isBinary=False):
        log.debug("(peer=%s) <-- sending %d bytes" % (self._peer, len(payload)))
        return super().sendMessage(payload, isBinary=isBinary)


class WSClientProtocol(WSProtocolBase, WebSocketClientProtocol):
    pass


class WSServerProtocol(WSProtocolBase, WebSocketServerProtocol):

    def __init__(self):
        super().__init__()

        self.processor = None
        self.client_registry = None
        self._conn_runner = None

    def onOpen(self):
        assert self.processor
        assert self.client_registry

        # der Transport muss hier nicht explizit geöffnet werden, da die Verbindung ja vom Client kam, also auf
        # TCP-Ebene schon steht und auf WebSocket-Ebene wurde dieser Task aus dem onOpened-Handler gestartet, also
        # der WebSocket ist auch schon betriebsbereit.
        transport = TBufferedWebSocketServerTransport(self)
        protocol = TBinaryMessageTypeDispatchingAcceleratedProtocol(transport)

        self._conn_runner = WebSocketConnectionRunner(transport, protocol, self.processor)
        self._conn_runner.start_async_task()

        self.client_registry.new_connection(protocol, self._peer)

        super().onOpen()

    def onClose(self, wasClean, code, reason):
        assert self.client_registry

        self._conn_runner.task.cancel()
        self.client_registry.drop_connection(self._peer)

        super().onClose(wasClean, code, reason)
