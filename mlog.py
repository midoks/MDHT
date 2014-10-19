#!/usr/bin/python
#encoding: utf-8

import urllib

class mlog(object):
    def __init__(self):
        pass
    def record(self, infohash):
        print infohash.encode("hex")
        #url = "http://localhost/dht/crawler.php?infohash=" + infohash.encode("hex")
        #self.req(url)
    def req(self, url):
        print urllib.urlopen(url).read()
