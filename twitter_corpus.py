# Copyright (C) 2012-2013 Wesley Baugh
#
# This work is licensed under the Creative Commons
# Attribution-NonCommercial-ShareAlike 3.0 Unported License.
# To view a copy of this license, visit:
# http://creativecommons.org/licenses/by-nc-sa/3.0/
#
# This file was created using the streaming example from Tweepy
# as a general guide. Tweepy is licensed under the MIT License
# and is Copyright (c) 2009-2010 Joshua Roesslein.
"""Collects all tweets from the sample Public stream using Twitter's
streaming API, and saves them to a file for later use as a corpus.

The sample Public stream "Returns a small random sample of all public
statuses. The Tweets returned by the default access level are the same,
so if two different clients connect to this endpoint, they will see the
same Tweets."

This module consumes tweets from the sample Public stream and putes them
on a queue. The tweets are then consumed from the queue by writing them
to a file in JSON format as sent by twitter, with one tweet per line.
This file can then be processed and filtered as necessary to create a
corpus of tweets for use with Machine Learning, Natural Language Processing,
and other Human-Centered Computing applications.
"""
from __future__ import print_function

import sys
import threading
import Queue
import time
import socket
import httplib

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy.utils import import_simplejson
import staticconf
json = import_simplejson()


# Configuration file that contains the Twitter API credentials.
CONFIG_FILE = 'config.yaml'

# Number of seconds to wait after an exception before restarting the stream.
tcpip_delay = 0.25
MAX_TCPIP_TIMEOUT = 16
http_delay = 5
MAX_HTTP_TIMEOUT = 320


class QueueListener(StreamListener):
    """Tweets received from the stream are stored in an internal queue.

    Get tweets from the internal queue directly in order to process them.
    For example, in this module the tweets are taken off the queue to be
    written to a file, however they could just as easily be processed on
    the fly.
    """
    def __init__(self):
        """Creates a new stream listener with an internal queue for tweets."""
        super(QueueListener, self).__init__()
        self.num_handled = 0
        self.queue = Queue.Queue()

    def on_data(self, data):
        """Routes the raw stream data to the appropriate method."""
        data_json = json.loads(data)
        if 'in_reply_to_status_id' in data_json:
            if self.on_status(data) is False:
                return False
        elif 'limit' in data_json:
            if self.on_limit(data_json['limit']['track']) is False:
                return False
        return True

    def on_status(self, data):
        """Puts each tweet in JSON format in the queue and increments count."""
        # Note that in this overridden method 'data' is a string whereas
        # in the default listener method this method is passed an object in the
        # format of: status = Status.parse(self.api, json.loads(data))
        self.queue.put(data)
        self.num_handled += 1

    def on_error(self, status):
        """Prints any error to the console but does not halt the stream."""
        print('ON ERROR:', status, file=sys.stderr)

    def on_limit(self, track):
        """Prints any limit notice to the console but doesn't halt.

        Limit notices indicate that additional tweets matched a filter,
        however they were above an artificial limit placed on the stream
        and were not sent. The value of 'track' indicates how many tweets
        in total matched the filter but were not sent since the stream
        was opened.
        """
        print('ON LIMIT:', track, file=sys.stderr)


def print_status(listener, seconds=5.0, last_count=0):
    """Call once to repeatedly display statistics every N seconds."""
    num_handled = listener.num_handled
    qsize = listener.queue.qsize()

    t = threading.Timer(seconds, print_status, args=[listener, seconds,
                                                     num_handled])
    t.daemon = True
    t.start()

    print('\nTOTAL TWEETS HANDLED:', num_handled, file=sys.stderr)
    print(
        'NUM IN THE PAST {seconds} seconds: {delta} ({rate} per sec)'.format(
            seconds=seconds,
            delta=num_handled - last_count,
            rate=(num_handled - last_count) / seconds,
        ),
        file=sys.stderr,
    )
    if qsize > 0:
        print('QUEUE SIZE:', qsize, file=sys.stderr)


def worker(listener, flush_every=500):
    """Takes tweets off of the queue and writes them to a file."""
    count = 0
    while True:
        data = listener.queue.get()
        if data is None:
            listener.queue.task_done()
            break
        try:
            print(data)
        except UnicodeDecodeError:
            print(
                'ERROR: UnicodeDecodeError ... continuing.',
                file=sys.stderr,
            )
        count += 1
        if count == flush_every:
            sys.stdout.flush()
            count = 0
        listener.queue.task_done()


def main():
    """Connects to the stream and starts threads to write them to a file."""
    staticconf.YamlConfiguration(CONFIG_FILE)
    listener = QueueListener()
    auth = OAuthHandler(
        staticconf.read_string('twitter.consumer_key'),
        staticconf.read_string('twitter.consumer_secret'),
    )
    auth.set_access_token(
        staticconf.read_string('twitter.access_token'),
        staticconf.read_string('twitter.access_token_secret'),
    )

    writer_thread = threading.Thread(target=worker, args=(listener,))
    writer_thread.start()

    stream = Stream(auth, listener)

    print_status(listener)

    try:
        while True:
            try:
                stream.sample()  # blocking!
            except KeyboardInterrupt:
                print('KEYBOARD INTERRUPT', file=sys.stderr)
                return
            except (socket.error, httplib.HTTPException):
                global tcpip_delay
                print(
                    'TCP/IP Error: Restarting after {delay} seconds.'.format(
                        delay=tcpip_delay,
                    ),
                    file=sys.stderr,
                )
                time.sleep(min(tcpip_delay, MAX_TCPIP_TIMEOUT))
                tcpip_delay += 0.25
    finally:
        print('Disconnecting stream', file=sys.stderr)
        stream.disconnect()
        print('Waiting for last tweets to finish processing', file=sys.stderr)
        # Send poison pill to writer thread and wait for it to exit
        listener.queue.put(None)
        listener.queue.join()
        print('Waiting for writer thread to finish', file=sys.stderr)
        writer_thread.join()
        print('Exit successful', file=sys.stderr)


if __name__ == '__main__':
    sys.exit(main())
