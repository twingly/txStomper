#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

sys.path.append("../../")

import logging.config

import twisted.internet.error
from twisted.internet import reactor, protocol, defer

from twistedstomp.protocol import StompProtocol, StompConnectError



def on_message(msg):
    print "on_message"
    print msg
    
@defer.inlineCallbacks
def connect_succeeded(result, conn):
    print "connect succeeded"
    
    print "subscribing..."
    yield conn.subscribe('/queue/inbox', on_message, ack='client')
    print "subscribed"

    print "subscribing again"
    yield conn.subscribe('/queue/inbox', on_message, ack='client')
    print "subscribed again (should not have sent anything to the server)"
    
    reactor.callLater(10, disconnect, conn)

@defer.inlineCallbacks
def disconnect(conn):
    print "unsubscribing"
    yield conn.unsubscribe('/queue/inbox')
    print "unsubscribed"
    
    print "disconnecting..."
    yield conn.disconnect()
    print "disconnected"
    
    reactor.stop()

def connection_established(conn, login, passcode):
    # try with the correct login and passcode...
    d = conn.connect(login, passcode)
    d.addCallback(connect_succeeded, conn)
    d.addErrback(connect_failed)

def connect_failed(reason):
    print "connect failed."
    print reason
    reactor.stop()

def connection_failed(reason):
    print "connect failed!"
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

