#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import uuid

from twisted.internet.defer import Deferred, fail, succeed
from twisted.internet.protocol import Protocol

import stomper



class StompError(Exception): pass

class StompConnectError(StompError):
    """
    The connect call failed, probably because of invalid login and/or passcode.
    """
    pass


_CONNECTED = 'CONNECTED'
_MESSAGE = 'MESSAGE'
_ERROR = 'ERROR'
_RECEIPT = 'RECEIPT'



class StompProtocol(Protocol):
    def __init__(self):
        self._connect_deferred = None
        self._disconnect_deferred = None
        
        self._disconnected = True
        self._session_id = ''
        
        self._stomp_buffer = stomper.stompbuffer.StompBuffer()
        self._destination_callbacks = {}
        self._receipt_deferreds = {}



    def _fail_outstanding_receipts(self, reason):
        for d in self._receipt_deferreds.itervalues():
            d.errback(reason)



    def connectionLost(self, reason):
        logging.debug("connection lost: %s" % reason)
        
        self._disconnected = True
        self._fail_outstanding_receipts(reason)
        
        if self._connect_deferred:
            d, self._connect_deferred = self._connect_deferred, None
            d.errback(reason)
        
        if self._disconnect_deferred:
            d, self._disconnect_deferred = self._disconnect_deferred, None
            d.callback(True)



    def dataReceived(self, data):
        self._stomp_buffer.appendData(data)
        
        while True:
           msg = self._stomp_buffer.getOneMessage()
           if msg is None:
               break
           
           cmd = msg['cmd']
           
           if cmd == _CONNECTED:
               self._cmd_connected(msg)
           elif cmd == _MESSAGE:
               self._cmd_message(msg)
           elif cmd == _ERROR:
               self._cmd_error(msg)
           elif cmd == _RECEIPT:
               self._cmd_receipt(msg)



    def connectionMade(self):
        pass



    def _cmd_connected(self, msg):
        self._session_id = msg['headers']['session']
        self._disconnected = False
        
        logging.debug("connected: session id '%s'." % self._session_id)
        
        if self._connect_deferred is not None:
            d, self._connect_deferred = self._connect_deferred, None
            d.callback(True)



    def _cmd_message(self, msg):
        destination = msg['headers']['destination']
        
        logging.debug("got message to destination <%s>." % destination)
        
        # thought we would always get a message-id, but it seems not
        # (at least from the ruby stompserver)
        message_id = None
        if msg['headers'].has_key('message-id'):
            message_id = msg['headers']['message-id']
        
        transaction_id = None
        if msg['headers'].has_key('transaction-id'):
            transaction_id = msg['headers']['transaction-id']
            
        # Call subscription callback with the message body
        self._destination_callbacks[destination](msg['body'].replace(stomper.NULL, ''))
        
        if message_id:
            logging.debug("acknowledging message id <%s>." % message_id)
            self._write(stomper.ack(message_id, transaction_id))



    def _cmd_error(self, msg):
        brief_msg = ""
        if msg['headers'].has_key('message'):
            brief_msg = msg['headers']['message']
        
        body = msg['body'].replace(stomper.NULL, '')
        error_msg = "Error: message: '%s', body: '%s'" % (brief_msg, body)
        logging.debug(error_msg)
        
        # If self._connect_deferred is set, then we haven't successfully connected, and if we
        # get an error then, call the errback function
        if self._disconnected and (self._connect_deferred is not None):
            reply = StompConnectError(brief_msg, body)
            d, self._connect_deferred = self._connect_deferred, None
            d.errback(reply)

        # We don't know what the error is about, so assume the worst.
        self._fail_outstanding_receipts(StompError(brief_msg, body))



    def _cmd_receipt(self, msg):
        brief_msg = ""
        if msg['headers'].has_key('receipt-id'):
            brief_msg = msg['headers']['receipt-id']
        
        logging.debug("Received server receipt message - receipt-id:%s\n\n%s" %
                      (brief_msg, msg['body'].replace(stomper.NULL, '')))
        
        receipt_id = None
        if msg['headers'].has_key('receipt-id'):
            receipt_id = msg['headers']['receipt-id']
        
        if receipt_id is not None:
            try:
                d = self._receipt_deferreds.pop(receipt_id)
                d.callback(True)
            except KeyError, err:
                # The server sent us a receipt that we weren't expecting. Fail the outstanding receipts
                # that we were waiting for and close the connection
                self._fail_outstanding_receipts(StompError("got receipt from server with unknown receipt-id"))
                self.disconnect()



    def _write(self, s):
        self.transport.write(s)



    def _generate_receipt_id(self):
        def generate_id():
            return str(uuid.uuid4())
        
        id = generate_id()
        while id in self._receipt_deferreds:
            id = generate_id()
        
        return id



    def _send_frame(self, stomp_message, receipt, timeout, keyword_headers):
        """
        Create and write a STOMP frame created by unpacking
        C{stomp_message} and adding the correct headers.
        """
        frame = stomper.Frame()
        frame.unpack(stomp_message)
        frame.headers.update(keyword_headers)
        
        receipt_id = None        
        # Check if the user supplied a receipt header, and use that
        # receipt id instead of generating one. If we already have an
        # outstanding receipt request with that id, fail.
        if frame.headers.has_key('receipt'):
            receipt = True
            receipt_id = frame.headers['receipt']
            if recept_id in self._receipt_deferreds:
                return fail(StompError("receipt id is already in use"))
        
        if receipt:
            if receipt_id is None: receipt_id = self._generate_receipt_id()
            frame.headers['receipt'] = receipt_id
            d = self._create_receipt_deferred(receipt_id, timeout)
        else:
            d = succeed(True)
        
        self._write(frame.pack())
        return d



    def _create_receipt_deferred(self, receipt_id, timeout):
        d = Deferred()
        # TODO: setTimeout in Deferred is deprecated and shouldn't be
        # used. The idea seem to be that it should be up to
        # application to handle that, and not Twisted.
        # http://twistedmatrix.com/pipermail/twisted-python/2004-April/007531.html
        if timeout > 0:
            d.setTimeout(timeout)
        self._receipt_deferreds[receipt_id] = d
        return d



    #################
    # STOMP commands
    #
    
    def subscribe(self, destination, on_message, receipt = True, timeout = 0, **keyword_headers):
        """
        Subscribe to a destination (queue).
        
        @param destination: The destination (queue) to subscribe to.
        
        @param on_message: The callback function that will be called
            for each message that arrives on the subscribed
            destination. The function should take exactly one
            argument; the message.
        
        @param keyword_headers: Custom headers to send to the server.
        """
        
        if self._disconnected:
            return fail(RuntimeError("not connected"))
        
        if destination in self._destination_callbacks:
            return succeed(False) # symbolize that the subscribe was ok, since we were already subscribed
        
        self._destination_callbacks[destination] = on_message
        
        return self._send_frame(stomper.subscribe(destination), receipt, timeout, keyword_headers)



    def unsubscribe(self, destination, receipt = True, timeout = 0, **keyword_headers):
        """
        Unsubscribe from a destination (queue).
        
        @param destination: The destination (queue) to subscribe to.
        """
        if self._disconnected:
            return fail(RuntimeError("not connected"))
        
        if destination not in self._destination_callbacks:
            return succeed(False) # symbolize that the unsubscribe was ok, since we weren't subscribed
        
        del self._destination_callbacks[destination]
        
        return self._send_frame(stomper.unsubscribe(destination), receipt, timeout, keyword_headers)



    def send(self, message, destination, receipt = True, timeout = 0, **keyword_headers):
        """
        Send a message to a destination.
        
        @param message: The message to send.
        
        @param destination: The destination (queue) to subscribe to.
        
        @param receipt: If the server should return a receipt on that
            it got the message (default True).
        
        @param timeout: How long to wait for a receipt from the
            server. Is only valid if C{receipt} is True.
        
        @param keyword_headers: Custom headers to send to the server.
        
        @return: A deferred. If C{receipt} is True, then it will
            return a deferred that will be called back with True, when
            the receipt from the server has arrived. If C{receipt} is
            False, it will return a defer.succeed(True).
        """
        if self._disconnected:
            return fail(RuntimeError("not connected"))
        
        return self._send_frame(stomper.send(destination, message), receipt, timeout, keyword_headers)



    def connect(self, login = '', passcode = ''):
        """
        Connect to the STOMP server with login and passcode. Must be
        done before any other commands can be sent.
        
        @param login: The login to send to the STOMP server. Default is empty string.
        
        @param passcode: The passcode to send to the STOMP server. Default is empty string.
        
        @return: A deferred that will be called with True if the connect was successful.
        """
        logging.debug("connecting with login: '%s' and passcode: '%s'" % (login, passcode))

        #if self._disconnected:
        #    return fail(RuntimeError("not connected"))
        
        self._write(stomper.connect(login, passcode))
        
        self._connect_deferred = Deferred()
        return self._connect_deferred



    def disconnect(self, **keyword_headers):
        """
        Disconnect from the server.
        
        @return: A deferred that will be called with True when the connection is closed.
        """
        if self._disconnected:
            return fail(RuntimeError("not connected"))
        
        # TODO: close connection hard after a timeout (self.transport.loseConnection())
        
        # we don't care about the deferred from sending the frame,
        # only when the server actually closes the connection.
        self._send_frame(stomper.disconnect(), False, 0, keyword_headers)
        
        self._disconnect_deferred = Deferred()
        return self._disconnect_deferred
