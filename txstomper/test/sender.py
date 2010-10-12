#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

sys.path.append("../../")

import logging.config
from twisted.internet import reactor, protocol, defer

from txstomper.protocol import StompProtocol



@defer.inlineCallbacks
def connect_succeeded(result, conn):
    print "connect succeeded"
    
    print "sending message..."
    yield conn.send('hej!', '/queue/inbox')
    
    
    print "disconnecting..."
    yield conn.disconnect()
    print "disconnected"
    reactor.stop()

def on_error(reason):
    print "got error"
    
    print reason

def connection_established(conn, login, passcode):
    d = conn.connect(login, passcode)
    d.addCallback(connect_succeeded, conn)
    d.addErrback(connect_failed)

def connection_failed(reason):
    print "connect failed!"
    print reason
    reactor.stop()

def connect_failed(reason):
    print "connect failed. wrong login or passcode?"
    print reason
    reactor.stop()

def start(host='localhost', port=61613, login='', passcode=''):
    print "starting..."

    clientCreator = protocol.ClientCreator(reactor, StompProtocol)
    d = clientCreator.connectTCP(host, port)
    d.addCallback(connection_established, login, passcode)
    d.addErrback(connection_failed)
    
    reactor.run()


if __name__ == '__main__':
    logging.config.fileConfig('logging.conf')
    start(login='oskar', passcode='hej123')
