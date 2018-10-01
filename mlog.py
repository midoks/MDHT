#!/usr/bin/python
#encoding: utf-8

import urllib

from simMetadata import download_metadata 

class mlog(object):
    def __init__(self):
        pass
    def record(self, infohash, address):
        print infohash.encode("hex"), address
        # download_metadata(infohash, address)
        #url = "http://localhost/dht/crawler.php?infohash=" + infohash.encode("hex")
        #self.req(url)
    def req(self, url):
        print urllib.urlopen(url).read()
