# -*- coding: utf-8 -*-
from collections import defaultdict, deque
from thrift.transport.TTransport import TMemoryBuffer
from thrift.Thrift import TMessageType


class ThriftMessageQueue:
    """
    Collects all incoming Thrift-Messages and wraps them already into a TMemoryBuffer-Transport.
    With the help of the given protocol factory the message begin will be read and so the message type will be snooped.
    """

    def __init__(self, tprotocol_factory):
        # enables us to read the message header to determine the message type
        self._tproto_fab = tprotocol_factory

        # key: Thrift.TMessageType, value: list of messages in order of recieving, already wrapped as
        # TProtocol(TMemporyTransport(message)), because we need it to snoop the message type and outside it will be
        # needed that way as well
        self._messages = defaultdict(deque)

    def add(self, message, log=None):
        mem_trans = TMemoryBuffer(message)
        iproto = self._tproto_fab.getProtocol(mem_trans)

        (name, type, seqid) = iproto.readMessageBegin()  # @ReservedAssignment @UnusedVariable

        # rewind so the buffer will be fresh again for the processor
        mem_trans._buffer.seek(0)

        # and safe the iproto object because this is what we need outside anyway
        self._messages[type].append(iproto)

        if log:
            log.debug("message of type %s at protocol layer queued" % TMessageType.get_name(type))  # @UndefinedVariable

    def get(self, *message_types):
        for mtype in message_types:
            try:
                return self._messages[mtype].popleft()
            except IndexError:
                continue

        raise EOFError()
