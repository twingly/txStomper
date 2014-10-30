txStomper is a STOMP client library for Twisted Python.

Note
====
This repository is _NOT_ maintained as we do not use STOMP at Twingly anymore.

Description
===========
txStomper is based on Stomper for creating and parsing STOMP messages. Stomper
includes examples on how to use Stomper with Twisted, but txStomper makes
better use of Deferreds and also provides support for receipts (optional).
When demanding receipts on sent messages, the returned Deferred won't succeed
until a receipt has been received for the message. You can also specify a
timeout for when the Deferred should fail, if a receipt hasn't been received
from the server.


Requirements
============
Stomper (http://code.google.com/p/stomper/).


License
=======
Copyright 2010 Twingly AB. txStomper is provided under the three-clause
BSD License. See the included LICENSE.txt file for specifics.


Example
=======
import twisted.internet.error
from twisted.internet import reactor, protocol, defer

from txstomper.protocol import StompProtocol, StompConnectError

def on_message(msg):
    print "on_message"
    print msg

@defer.inlineCallbacks
def start:
    clientCreator = protocol.ClientCreator(reactor, StompProtocol)
    conn = yield clientCreator.connectTCP(host, port)
    yield conn.connect(login, passcode)
    yield conn.subscribe('/queue/inbox', on_message, ack='client')
    yield conn.send('hello world', '/queue/something')
    yield conn.send('hello world', '/queue/somethingelse', receipt=False)
    yield conn.unsubscribe('/queue/inbox')
    yield conn.disconnect()
