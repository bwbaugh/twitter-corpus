General Info
============

Collects all tweets from the sample Public stream using Twitter's
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

Required Libraries
==================

### Tweepy
* A Python library for accessing the Twitter API.
* Does all the heavy lifting of connecting to the sample Public stream.
* Available at: <https://github.com/tweepy/tweepy>
