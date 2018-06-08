# -*- coding: utf-8 -*-
import asyncio
import logging
from io import BytesIO
from thrift.transport import TTransport
from collections import deque
import time

log = logging.getLogger("thrift.wsasync.ttransport")
log.setLevel(logging.WARNING)


class TBufferedWebSocketTransportBase(TTransport.TTransportBase):
    """
    adaptiert WebSocketClientProtocol von und zu einem Thrift-Transport. Kommt dabei nicht drumrum, gleich
    die Buffered-Funktionalität eines TBufferedTransport mit zu implementieren. Im write-Verhalten ist diese dazu
    identisch. Das read-Verhalten ist hier derart, als dass wir Daten aus der WebSocket-Schicht unter uns entgegen
    nehmen, wie sie reinkommen und puffern und hier auf Anfrage nach oben weiterreichen. Der TBufferedTransport
    tickt da so, dass er bei einem read()-Call auch wie wir hier schaut, ob er noch Bytes in seinem Buffer hat und dann
    diese zurückgibt. Hat er hingegen nichts im Buffer, ruft er selber TTransport.read(), den er wrappt, um wieder
    an Daten zu kommen (und da dann gleich so viele wie gewünscht, mindestens aber so viele um seinen Buffer voll zu
    füllen). Das können wir hier so nicht machen, denn wir haben keine Möglichkeit direkt einen read()-Call auf einem
    Websocket aufzurufen. Bei uns befüllt der sich per Callback vom WebSocketClientProtocol über _on_recieved_data().
    """

    def __init__(self, ws_protocol):
        # das asyncio-Connection-Protokoll, für welches wir via autobahn.WebSocketClientProtocol die
        # WebSocket-Sicht präsentiert bekommen
        self._p = ws_protocol

        # read buffer
        self._recieved_messages = deque()
        self._p.set_on_message_callback(self._on_recieved_data)
        self._new_msg_event = asyncio.Event()
        self.msg_processing_finished = asyncio.Event()
        self.msg_processing_finished.set()

        # write buffer
        self._wbuf = BytesIO()

    def _on_recieved_data(self, data):
        """
        `data` ist immer eine gesamte, abgeschlossene Thrift-Message und entspricht dem blob, welches auf ws-Protokoll-
        Ebene im onMessage() reinkam. Das setzt voraus, dass die Gegenseite eine gesamte Thrift-Message mittels eines
        sendMessage()-Calls abgesetzt hat (ergo der flush() auf dem ThriftTransport sollte immer am Ende des Message-
        Serialisierens stattfinden!) als auch, dass wir davon ausgehen, dass die Daten, die die sendende Seite in
        sendMessage() auf dem ws-Protokoll abschickt, auch exakt 1:1 in einen onMessage()-Call auf der empfangenden
        Seite abkommen. Von letzteren gehe ich nach Tests diesbzgl. aus und ersteres haben wir selber in der Hand.
        Diese Vereinfachung hilft uns enorm beim Deserialisieren, da wir hier das AcceleratedProtokoll einmal mit der
        gesamten Message "füttern" können, anstatt wegen jedes einzelne read-Calls immer zurück in die Python-Welt
        zu callen.
        """
        self._recieved_messages.append(data)
        self._new_msg_time = time.time()
        self._new_msg_event.set()
        log.debug("recieved message (%d bytes) at ttranspot layer - new_msg_event set!" % len(data))

    def read(self, sz):
        raise NotImplementedError("intentionally - see comments")

    def readAll(self, sz):
        raise NotImplementedError("intentionally - see comments")

    def close(self):
        self._p.sendClose(code=1000)
        self._p.set_on_message_callback(None)

        self._recieved_messages = deque()
        self._new_msg_event.clear()
        self._wbuf = BytesIO()

    def write(self, buf):
        """
        übernommen aus TBufferedTransport
        """
        try:
            self._wbuf.write(buf)
        except Exception as e:
            # on exception reset wbuf so it doesn't contain a partial function call
            self._wbuf = BytesIO()
            raise e

    def flush(self):
        """
        teilweise übernommen aus TBufferedTransport
        """
        out = self._wbuf.getvalue()
        # reset wbuf before write/flush to preserve state on underlying failure
        self._wbuf = BytesIO()

        # und ab in den Websocket damit
        self._p.sendMessage(out, isBinary=True)

        # !!! folgendes wird nicht mehr gehen, da wir voraussetzen, dass eine Message immer im Gesamten über die ws-
        # !!! Leitung gesendet wird
        # --- emulierte Fragmentierung ---
        # middle = len(out) // 2
        # self._p.sendMessage(out[:middle], isBinary=True)
        # self._p.sendMessage(out[middle:], isBinary=True)

    # --- unser eigenes read-Interface ---

    def get_next_message(self):
        try:
            return self._recieved_messages.popleft()
        except IndexError:
            raise EOFError()

    async def wait_for_message(self):
        log.debug("waiting for further message...")
        self._new_msg_event.clear()
        await self._new_msg_event.wait()
        log.debug("wait_for_message on ttransport succeeded!")


class TBufferedWebSocketClientTransport(TBufferedWebSocketTransportBase):
    """
    Auf Client-Seite hat die Transport-Schicht explizit Funktionalität zum Öffnen des Transports. Dabei Verbinden wir
    uns mit der Gegenseite. Das dabei erhaltene asyncio-Socket-Protokoll setzt das autobahn-Package mit deren
    WebSocketProtocol-Klassen um und bringt es auch ws-Protokoll-Ebene. Das ist dann die Schicht, die gedanklich unter
    unserem Thrift-Transport liegt, d.h. dieser befüllt sich dann per Callback durch das ws-protocol.onMessage() bzw.
    flusht seine Daten in ws-protocol.sendMessage().
    """

    CONNECT_TIMEOUT = 5

    def __init__(self, ws_protocol, loop):
        super().__init__(ws_protocol)

        # die asyncio-loop
        self._l = loop

        # der asyncio-Connection-Transport, welche wir von loop.create_connection() erhalten
        self._t = None

    async def open(self):

        async def connect():
            (t, p) = await self._l.create_connection(lambda: self._p, '127.0.0.1', 9000)
            assert p is self._p
            self._t = t

        # raise OSError, ConnectionRefusedError und dergleichen
        await asyncio.wait_for(connect(), self.CONNECT_TIMEOUT)

    def close(self):
        super().close()

        if self._t:
            self._t.close()


class TBufferedWebSocketServerTransport(TBufferedWebSocketTransportBase):
    """
    Die Serverseite bedarf keines explizen open()-Calls, denn hier wird per loop.create_server() bereits auf
    Verbindungen per bind gewartet.
    """

    def __init__(self, ws_protocol):
        super().__init__(ws_protocol)
