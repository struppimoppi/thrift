#-*- coding: utf-8 -*-
from io import BytesIO, SEEK_END
import time
import threading
import logging

from ws4py.client.threadedclient import WebSocketClient
from ws4py.exc import HandshakeError

from .TTransport import TTransportBase
from ..protocol import TBinaryProtocol

USE_FAST_BINARY = True
if USE_FAST_BINARY:
    BINARY_PROTOCOL_CLASS = TBinaryProtocol.TBinaryProtocolAccelerated
else:
    BINARY_PROTOCOL_CLASS = TBinaryProtocol.TBinaryProtocol
    
def _trace(logger, *args, **kwargs):
    if logger.level > 0:
        return
    f = logger.debug
    if hasattr(f, 'trace'):
        f = logger.trace
    f(*args, **kwargs)

class TWebSocketTransport(TTransportBase):
    
    def __init__(self, url, connected_event=None):
        self._connected_event = connected_event or threading.Event()
        self._url = url
        self._ws = None
        
    def isOpen(self):
        return self._ws and self._ws.connected and self._ws.open

    def open(self):
        if not self._ws or not self._ws.connected:
            self._ws = TBufferedWebSocket(self._url, opend_event=self._connected_event)
            self._ws.connect()

    def close(self, reason=""):
        if self._ws and self._ws.connected:
            self._ws.close(reason=reason)
    
    def read(self, sz):
        return self._ws.read(sz)
    
    def write(self, buf):
        self._ws.write(buf)
    
    def flush(self):
        self._ws.flush()
        
    def maybe_purge_current_message_data(self):
        """
        Falls beim Verarbeiten einer Message ein Fehler auftritt, wird diese Message ggf. nicht vollständig ausgelesen.
        Mit dieser Methode wird veranlasst, dass die aktuelle Message, in der gerade gelesen wird, verwurfen wird.
        """
        if self._ws:
            self._ws.maybe_purge_current_message_data()
    
class ConnectionClosedError(Exception): pass

class TBufferedWebSocket(WebSocketClient):
    
    MAX_BUFFER_READ_SIZE = 16384
    
    logger = logging.getLogger("TBufferedWebSocket")
    logger.setLevel(logging.DEBUG)

    def __init__(self, url, opend_event=None):
        WebSocketClient.__init__(self, url)
        
        self._opend_event = opend_event or threading.Event()
        
        # die eintreffenden Messages speichern wir in einer Liste. 
        self._recieved_messages = []
        
        # die Leseposition in der ersten Message in der Liste
        self._read_pos_msg0 = 0
        
        self._inbuf = None
        
        self._revieved_data_condition = threading.Condition()

    def opened(self):
        """
        Override aus WebSocketClient. Der Socket wurde geöffnet.
        """
        self._inbuf = BytesIO()
        self._opend_event.set()
        
    @property
    def open(self):
        return self._opend_event.is_set()

    def closed(self, code, reason=None):
        """
        Override aus WebSocketClient. Der Socket wurde geschlossen.
        """
        self._opend_event.clear()

    def received_message(self, m):
        """
        Override aus WebSocketClient. Daten wurden empfangen.
        """
        self.logger.debug("received_message: len(m)=%d len(received_messages)=%d" % (len(m.data), len(self._recieved_messages)))
        if len(m.data) == 0:
            return

        with self._revieved_data_condition:
            _trace(self.logger, "received_message: have revieved_data_mutex")

            self._recieved_messages.append(m.data)
        
            _trace(self.logger, "received_message: ready -> notify revieved_data")    
            self._revieved_data_condition.notify()
        
    #--- API for TWebSocketTransport
        
    def read(self, sz):
        """
        Diese Methode muss so lange blocken, bis sz bytes ausgeliefert werden können.
        """
        with self._revieved_data_condition:
            while 1:
                if not self._opend_event.is_set():
                    raise ConnectionClosedError()
                
                # ist überhaupt eine Message vorrätig?
                if not self._recieved_messages:
                    _trace(self.logger, "read(%d) - no message available" % sz)
                    self._revieved_data_condition.wait(timeout=1)
                    continue
                
                cur_msg = self._recieved_messages[0]
                
                if self._read_pos_msg0 == 0:
                    _trace(self.logger, "start reading %d bytes from new msg (len=%d): %s (%s)" % (sz, len(cur_msg), cur_msg[:1000], get_current_stack()))
                
                # hat die aktuelle Message noch sz Bytes?
                if len(cur_msg) - self._read_pos_msg0 < sz:
                    raise Exception("Es sollen %d Bytes gelesen werden, aktuelle Message hat aber nur noch %d Bytes!" % (sz, len(cur_msg) - self._read_pos_msg0))
                
                data = cur_msg[self._read_pos_msg0 : self._read_pos_msg0 + sz]
                self._read_pos_msg0 += sz
                
                if len(cur_msg) == self._read_pos_msg0:
                    # Message wurde ausgelesen
                    del self._recieved_messages[0]
                    self._read_pos_msg0 = 0

                _trace(self.logger, "reading %d bytes" % len(data))
                return data
            
    def maybe_purge_current_message_data(self):
        if self._read_pos_msg0 == 0:
            _trace(self.logger, "nichts zu purgen")
            # es gibt keine Message, innerhalb  derer wir gerade Daten herausgeben
            return

        with self._revieved_data_condition:
            if len(self._recieved_messages) != 0:
                del self._recieved_messages[0]
                self._read_pos_msg0 = 0
                self.logger.debug("Restdaten der aktuellen Message wurden verwurfen!")

    def write(self, data):
        self._inbuf.write(data)
        
    def flush(self):
        data = self._inbuf.getvalue()
        self.send(data, binary=True)
        self._inbuf = BytesIO()
        
class DisconnectedError(Exception): pass

class TWebSocketManager(threading.Thread):
    
    logger = logging.getLogger("TWebSocketManager")
    logger.setLevel(logging.DEBUG)
    
    def __init__(self, url, client_processor, server_client, seqidman, num_connect_tries=10, on_connection_breakdown=None):
        threading.Thread.__init__(self, name="WebSocket Dispatcher")
        
        self._socket_connected = threading.Event()
        self._twstrans = TWebSocketTransport(url, connected_event=self._socket_connected)
        self._dispatcher = TWebSocketDispatcher(client_processor)
        self._num_connect_tries = num_connect_tries
        self._seqidman = seqidman
        self._server_client = server_client
        self._on_connection_breakdown = on_connection_breakdown
        
        self.last_connect_error = None
        self.connect_done = threading.Event()
    
    def run(self):
        try:
            self._connect()
            self.connect_done.set()
            
            if self.ready:
                self._listen()
        finally:
            if self._on_connection_breakdown:
                self._on_connection_breakdown()
        
    def stop(self, reason="good bye", timeout=None):
        self._on_connection_breakdown = None        # wenn mutwillig die Verbindung geschlossen wird, zählt das nicht als Verbindungsabbruch
        self._twstrans.close(reason=reason)         # löst eine ConnectionClosedException in unserem Thread aus
        self.join(timeout)
        
    @property
    def ready(self):
        return self._twstrans.isOpen()
    
    def _connect(self):
        tries = 0
        while 1:
            tries += 1
            try:
                self.logger.info("establishing connection")
                self._twstrans.open()
                self._socket_connected.wait()
                self.logger.info("connected")
                
                break
            except Exception as exp:
                self.logger.error("%s: %s" % (exp.__class__.__name__, exp))
                last_connect_error = exp
                if tries == self._num_connect_tries:
                    break
                time.sleep(1)
                
    def _listen(self):
        """
        Diese Methode hört auf eingehende Nachrichten und dispatcht diese dann entweder zu einem von uns gestellten Request, der auf
        seinen Response wartet oder zum ClientService-Processor, um einen Push vom Server zu verarbeiten.
        """
        protocol = BINARY_PROTOCOL_CLASS(self._twstrans)
        
        def dispatch_awaiting_response(message_begin):
            client_id = self._seqidman.get_client_id(message_begin[2])
            self.logger.debug("dispatch awaiting response: Thread/ClientId=%d, message_begin=%s" % (client_id, str(message_begin)))
            self._dispatcher.on_awaiting_response(client_id, message_begin)
            
            # hier noch sicherstellen, dass eine unfertig gelesene Message abgeräumt wird, für den Fall, dass in dem Thread beim Verarbeiten/Auslesen
            # der Message eine Exception geflogen ist und demnach noch Daten unausgelesen sind.
            self._twstrans.maybe_purge_current_message_data()
        
        def dispatch_service_call(message_begin):
            client_id = self._seqidman.get_client_id(message_begin[2])
            origin_id = message_begin[2] - client_id
            self.logger.debug("dispatch service call: OriginId=%d, Thread/ClientId=%d" % (origin_id, client_id))
            try:
                self._dispatcher.on_service_call(message_begin, protocol)
            finally:
                # hier noch sicherstellen, dass eine unfertig gelesene Message abgeräumt wird, für den Fall, dass hier eine Exception fliegt, siehe dispatch_awaiting_response
                self._twstrans.maybe_purge_current_message_data()
        
        while 1:
            try:
                _trace(self.logger, "*** vor WebSocketManager._listen/protocol.readMessageBegin: %d" % (len(self._twstrans._ws._recieved_messages)))
                (fname, mtype, rseqid) = protocol.readMessageBegin()

                if self._seqidman.is_awating_response(rseqid):
                    dispatch_awaiting_response((fname, mtype, rseqid))
                else:
                    dispatch_service_call((fname, mtype, rseqid))
            
            except Exception as exp:
                if isinstance(exp, ConnectionClosedError):
                    self.logger.warn("connection closed")
                    break                                                       # while-Schleife beenden
                    
                if not 'fname' in locals():
                    fname = '-unknown-'
                self.logger.error("fname='%s'" % fname, exc_info=exp)

    @property
    def server(self):
        """
        Liefert den Client-Stub, auf dem die entfernten Funktionen aufgerufen werden können.
        """
        if not self.ready:
            raise DisconnectedError()
        
        thread_data = threading.local()
        if not hasattr(thread_data, 'client_id'):
            thread_data.client_id = get_next_client_id()
        
        protocol = BINARY_PROTOCOL_CLASS(self._twstrans)
        self.logger.debug("erstelle client-stub: OriginId=%d Thread/ClientId=%d" % (self._seqidman.origin_id, thread_data.client_id))
        return self._server_client(self._seqidman.origin_id, thread_data.client_id, self._dispatcher, protocol)


class TWebSocketDispatcher(object):
    
    logger = logging.getLogger("TWebSocketDispatcher")
    logger.setLevel(logging.DEBUG)

    def __init__(self, service_processor):
        self._awt_condition = threading.Condition()
        self._awaiting_responses = {}
        self._service_processor = service_processor
        
    def register_awaiting_response(self, client_id, response_ready):
        with self._awt_condition:
            if client_id in self._awaiting_responses:
                raise Exception("Dieses ServiceStub/Client-Id %s ist bereits für einen erwarteten Response registriert!" % client_id)
    
            self._awaiting_responses[client_id] = response_ready
            _trace(self.logger, "register_awaiting_response: client_id %d registered" % client_id)
            self._awt_condition.notify()
    
    def on_awaiting_response(self, client_id, message_begin):
        with self._awt_condition:
            while not client_id in self._awaiting_responses:
                # das kommt vor, wenn der Response eintrifft noch bevor im Code des Services der erwwartete Response registriert wurde
                _trace(self.logger, "on_awaiting_response: client_id %d ist noch nicht registriert, warte..." % client_id)
                self._awt_condition.wait(timeout=1)
            
            response_ready = self._awaiting_responses.pop(client_id)    
            response_ready.set(message_begin)
            _trace(self.logger, "_handle_awaited_reponse: response_ready SET (message_begin=%s) & WAIT for processing" % str(message_begin))

            response_ready.response_processed.wait()
            _trace(self.logger, "_handle_awaited_reponse: processed")

    def on_service_call(self, message_begin, iprot, oprot=None):
        if oprot is None:
            oprot = iprot
        _trace(self.logger, "on_service_call: before process")
        self._service_processor.process(iprot, oprot, message_begin=message_begin)
        _trace(self.logger, "on_service_call: processed")

_client_id_counter = 0
_client_id_counter_mutex = threading.BoundedSemaphore()

def get_next_client_id():
    global _client_id_counter
    with _client_id_counter_mutex:
        _client_id_counter += 1
        return _client_id_counter


def get_current_stack():
    import traceback

    traces = traceback.extract_stack()[:-1]
    ret = "Stacktrace: "
    traces.reverse()

    for trace in traces:
        filename, lineno, module, text = trace  # @UnusedVariable
        if traces.index(trace) > 0:
            ret += " <== "
        ret += filename + '@' + module + '#' + str(lineno)
    return ret
