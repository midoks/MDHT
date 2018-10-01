#!/usr/bin/python
#encoding: utf-8
# DHTcrawler
# 参考:




import socket
import sys, os
from hashlib import sha1
from random import randint
from struct import unpack, pack
from socket import inet_aton, inet_ntoa
from threading import Timer, Thread, RLock
from time import sleep
from bencode import bencode, bdecode


import mlog


BOOTSTRAP_NODES = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881)
]
TID_LENGTH = 4
KRPC_TIMEOUT = 1
MAX_NODE_QSIZE = 10000
DHT_PORT = 6881
DHT_PID_NAME = "mdht.pid"


""" 定时器 """
def timer(t, f):
    Timer(t, f).start()

""" 存储数据的表 """
class KTable():
    def __init__(self, mdht):
        self.nid = mdht.random_id()
        self.nodes = []

    def put(self, node):
        self.nodes.append(node)

""" 节点模型 """
class KNode(object):
    def __init__(self, nid, ip=None, port=None):
        self.nid = nid
        self.ip = ip
        self.port = port

    def __eq__(self, node):
        return node.nid == self.nid

    def __hash__(self):
        return hash(self.nid)

""" 基础操作 """
class mdht():
    """初始化方法"""
    def __init__(self, ip = "0.0.0.0", port = 6881):
        self.ip = ip
        self.port = port

        self.ufd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.ufd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ufd.bind((self.ip, self.port))

        #初始化表
        self.table = KTable(self)
        self.max_node_qsize = MAX_NODE_QSIZE

        self.types = {
            "r": self.response_received,
            "q": self.query_received
        }
        self.actions = {
            "get_peers": self.get_peers_received,
        }

        self.mlog = mlog.mlog()
        timer(KRPC_TIMEOUT, self.timeout)

    """ 基本的功能 """
    def entropy(self, bytes):
        s = ""
        for i in range(bytes):
            s += chr(randint(0, 255))
        return s

    """ 产生随机数 """
    def random_id(self):
        hash = sha1()
        hash.update( self.entropy(20) )
        return hash.digest()

    """ 获取相邻的节点 """
    def get_neighbor(self, target):
        return target[:10] + self.random_id()[10:]

    def decode_nodes(self, nodes):
        n = []
        length = len(nodes)
        if (length % 26) != 0: 
            return n
        
        for i in range(0, length, 26):
            nid = nodes[i:i+20]
            ip = inet_ntoa(nodes[i+20:i+24])
            port = unpack("!H", nodes[i+24:i+26])[0]
            n.append( (nid, ip, port) )
        return n

    """ 发送KRPC消息 """
    def send_krpc(self, msg, address):
        try:
            self.ufd.sendto(bencode(msg), address)
        except:
            pass

    """ 接受find_node请求 """
    def response_received(self, msg, address):
        try:
            nodes = self.decode_nodes(msg["r"]["nodes"])
            for node in nodes:
                (nid, ip, port) = node
                if len(nid) != 20: continue
                if ip == self.ip: continue
                self.table.put( KNode(nid, ip, port) )
        except KeyError:
            pass

    """ 接受信息 """
    def query_received(self, msg, address):
        try:
            self.actions[msg["q"]](msg, address)
        except KeyError:
            pass
    
    """ 接受到peer的请求 """
    def get_peers_received(self, msg, address):

        try:
            self.send_krpc(msg, address)
            infohash = msg["a"]["info_hash"]
            self.mlog.record(address, infohash)
        except Exception, e:
            pass

    """ DHT的功能 """

    """ 查找节点,从而加入DHT网络节点 """
    def find_node(self, address, nid=None):
        nid = self.get_neighbor(nid) if nid else self.table.nid
        tid = self.entropy(TID_LENGTH)
        msg = {
            "t": tid,
            "y": "q",
            "q": "find_node",
            "a": {"id":nid, "target": self.random_id()}
        }
        self.send_krpc(msg, address)

    """ 加入到DHT网络中 """
    def joinDHT(self):
        for address in BOOTSTRAP_NODES:
            self.find_node(address)
            
    def timeout(self):
        if not self.table.nodes:
            self.joinDHT()
        timer(KRPC_TIMEOUT, self.timeout)

    """ 启动 """
    def run(self):
        self.joinDHT()
        while True:
            try:
                (data, address) = self.ufd.recvfrom(65536)
                msg = bdecode(data)
                self.types[msg["y"]](msg, address)
            except Exception:
                pass

    """ 模拟发送 """     
    def roam(self):
        while True:
            #print len(self.table.nodes)
            for node in list(set(self.table.nodes))[:self.max_node_qsize]:
                self.find_node((node.ip, node.port), node.nid)
            self.table.nodes = []
            sleep(1)

""" 线程控制类 """
class MThread(Thread):
    def __init__(self):
        self.DHT = mdht("0.0.0.0", DHT_PORT)
        Thread.__init__(self)

    def run(self):
        self.DHT.run()

    def spider(self):
        self.DHT.roam()

""" 写入PID """
def write_pid(dir, pid):
    f = open(dir + "/" +DHT_PID_NAME, "w")
    f.write(str(pid))
    f.close()

def read_pid(dir):
    f = open(dir + "/" + DHT_PID_NAME, "rb")
    pid = f.read()
    return pid


""" 删除PID """
def delete_pid(dir):
    os.remove(dir + "/" +DHT_PID_NAME)


""" 帮助信息 """
def showHelp():
    print "start --开启deamon进程"
    print "stop  --停止deamon进程"
    print "test  --测试DHT"

""" 启动DHT """
def main_start():
    t = MThread()
    t.start()
    t.spider()

""" 保存PID """
def deamon_pid():
    dir = os.getcwd()
    try:
        pid = os.fork()
        if pid > 0 :
            sys.exit(0)
    except OSError , e:
        print >> sys.stderr, "fork #1 failed: %d (%s)" % (e.errno, e.strerror)
        sys.exit(1)

    os.chdir("/")
    os.setsid()
    os.umask(0)
    try:
        pid = os.fork()
        if pid > 0 :
            print "Daemon PID %d" % (pid)
            #print dir + "/" + DHT_PID_NAME, pid
            write_pid(dir, pid)
            sys.exit(0)
    except OSError , e :
            print >> sys.stderr, "fork #1 failed: %d (%s)" % (e.errno, e.strerror)
            sys.exit(1)

""" 删除PID """
def deamon_pid_del():
    dir = os.getcwd()
    pid = read_pid(dir)
    cmd = "kill %d" % int(pid)
    os.system(cmd)
    delete_pid(dir)

def main():
    alen = len(sys.argv)
    if 2 == alen:
        #print sys.argv
        aargv = sys.argv[1]
        if aargv == "start":
            deamon_pid()
            main_start()
        elif aargv == "stop":
            deamon_pid_del()
        elif aargv == "test":
            main_start()
    else:
        showHelp()


if __name__ == "__main__":
    main()





