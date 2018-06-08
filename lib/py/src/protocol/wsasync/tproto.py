# -*- coding: utf-8 -*-
import logging
from thrift.Thrift import TMessageType
from thrift.protocol.TBinaryProtocol import TBinaryProtocol, TBinaryProtocolAccelerated, TBinaryProtocolFactory, \
                                            TBinaryProtocolAcceleratedFactory

from thrift.protocol.wsasync.tmsgqueue import ThriftMessageQueue
from thrift.wsasync import stack_info

log = logging.getLogger("thrift.wsasync.tproto")
log.setLevel(logging.WARNING)


class TBinaryMessageTypeDispatchingProtocolMixin:
    """
    bietet async-wait-Funktionalitäten an, auf alle Message-Typen, so dass die beiden Stellen im Programm darauf
    awaiten können. Dazu muss es in der Lage sein den MessageTyp der nächsten Message zu kennen, wozu es vom
    drunterliegenden Transport die ersten Bytes anfordern und per readMessageBegin() den MessageType erschnüffeln kann.
    
    Wir nutzen hier auch das im Thrift schon vorgehaltene Konzept, dass im generiertem Code zwischen Input- und
    OutputProtocol unterschieden werden kann dahingehend, dass wir hier eine neue Instanz des Input-Protocols liefern,
    welche einen TMemoryTransport darunter geklemmt bekommt, mit der Message, die wir hier, wenn sie denn type-korrekt
    vorliegt, befüllen. Aus genau diesem Grund hat unser TBufferedWebSocketTransport, der ansonsten dem benutzen
    TProtocol zu Grund liegt und auch immer als Output-Transport genutzt wird, keine read() und readAll()-Methoden
    implementiert, sondern kann nur ganze Messages rausgeben, wenn sie vorliegen.
    """

    def __init__(self, proto_factory):
        self.__messages = ThriftMessageQueue(proto_factory)

    async def wait_for_message(self, *message_types):

        while 1:
            try:
                # raises EOFError, wenn keine neue Message in der Transportschicht vorliegt
                msg = self.trans.get_next_message()
                log.debug("new message at ttransport layer")

                # in unsere Queue einsortieren und dabei auch den MessageType bestimmen
                self.__messages.add(msg, log=log)

            except EOFError:
                log.debug("no message at protocol layer while waiting for %s (%s) - wait..." % \
                          ([TMessageType.get_name(x) for x in message_types],  # @UndefinedVariable
                           stack_info(top_level_package_name='thrift_ws')))
                await self.trans.wait_for_message()
                # hier kein continue, sondern gleich direkt weiter probieren, ob die Message nun schon
                # hier eingequeed wurde, denn wir warten ja von zwei Stellen im Programm aus auf eine
                # neue Nachricht und die erste Stelle wird sie schon queuen, auch wenn sie nicht für diese
                # stelle bestimmt war

            try:
                # raises EOFError, wenn keine Message unseres gewünschten Types vorliegt
                msg_iproto = self.__messages.get(*message_types)

                # hier gibts die Message gewrappt als TProtocol(TMemoryBuffer(msg)) zurück
                return msg_iproto
            except EOFError:
                continue


class TBinaryMessageTypeDispatchingProtocol(TBinaryMessageTypeDispatchingProtocolMixin, TBinaryProtocol):

    def __init__(self, trans, strictRead=False, strictWrite=True, **kwargs):
        TBinaryProtocol.__init__(self, trans, strictRead=strictRead, strictWrite=strictWrite, **kwargs)

        proto_factory = TBinaryProtocolFactory(strictRead=strictRead, strictWrite=strictWrite, **kwargs)
        TBinaryMessageTypeDispatchingProtocolMixin.__init__(self, proto_factory)


class TBinaryMessageTypeDispatchingAcceleratedProtocol(TBinaryMessageTypeDispatchingProtocolMixin, TBinaryProtocolAccelerated):

    def __init__(self, *args, **kwargs):
        TBinaryProtocolAccelerated.__init__(self, *args, **kwargs)

        fabargs = args[1:]  # erstes Arg ist der Transport!
        proto_factory = TBinaryProtocolAcceleratedFactory(*fabargs, **kwargs)
        TBinaryMessageTypeDispatchingProtocolMixin.__init__(self, proto_factory)
