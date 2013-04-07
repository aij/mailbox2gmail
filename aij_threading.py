#!/usr/bin/python

# Simple and incomplete threading library inspired by CML.
# Copyright (C) 2013 Ivan Jager
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import threading
import time


class ChannelClosed(Exception):
    pass

"""Synchronous message passing channel.

This is a python implementation of the channels from CML (Concurrent ML).
http://cml.cs.uchicago.edu/pages/cml.html
"""
class Channel(object):
    def __init__(self):
        self.__lock = threading.Lock()
        self.__senders = []
        self.__receivers = []
        self.__closed = False

    """Send a meessage. The calling thread blocks until it is received."""
    def send(self, msg):
        with self.__lock:
            self.__checkopen()
            if self.__receivers:
                r = self.__receivers.pop()
                # We could unlock here.
                r.__msg = msg
                r.set()
                return
            else:
                ev = threading.Event()
                ev.__msg = msg
                self.__senders.insert(0, ev)
        ev.wait()
        self.__checkopen()


    """Receive a sent message, or block until one is sent."""
    def recv(self):
        with self.__lock:
            self.__checkopen()
            exists = bool(self.__senders)
            if exists:
                ev = self.__senders.pop()
            else:
                ev = threading.Event()
                self.__receivers.insert(0, ev)
        if exists:
            ev.set()
        else:
            ev.wait()
        try:
            return ev.__msg
        except AttributeError:
            self.__checkopen()
            raise



    def close(self):
        with self.__lock:
            self.__closed = True
            for l in (self.__receivers, self.__senders):
                for e in l:
                    e.set()

        
    def __checkopen(self):
        if self.__closed:
            raise ChannelClosed('channel has been closed')


"""A simple threadpool using Channels to send work to threads."""
class ThreadPool(object):
    def __init__(self, num_threads):
        self.num_threads = num_threads
        self.channel = Channel()
        self.sent = 0
        self.completed = 0
        self.__threads = []
        self.__closed = False
        for i in range(num_threads):
            t = threading.Thread(target=self.__do_work)
            t.start()
            self.__threads.append(t)

    def run(self, work):
        if self.__closed:
            raise ValueError('Thread pool closed.')
        self.sent += 1
        self.channel.send(work)

    
    def __do_work(self):
        try:
            while True:
                self.channel.recv()()
                self.completed += 1
        except ChannelClosed:
            pass

    """Finish outstanding work and shutdown the thead pool."""
    def close(self):
        self.__closed = True
        while self.sent != self.completed:
            print self.sent, self.completed
            if not [t for t in self.__threads if t.is_alive()]:
                print 'ThreadPool: all threads are dead :('
                break  # no sense waiting forever...
            time.sleep(1)  # FIXME dirty hack.
        self.channel.close()


if __name__ == "__main__":
    import sys
    t = ThreadPool(5)
    for x in sys.stdin.xreadlines():
        def w(s):
            def c():
                sys.stdout.write(s)
            return c
        t.run(w(x))

    print 'Closing thread pool'
    t.close()
