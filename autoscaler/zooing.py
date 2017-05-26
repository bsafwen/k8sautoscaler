'''
Created on May 26, 2017

@author: bsafwene
'''
from kazoo.client import KazooClient

class Zoo(object):
    def __init__(self,**kwargs):
        """
        :param
        """
        self.host = kwargs['host']
        self.port = kwargs['port']
        self.zk = KazooClient(hosts=self.host+":"+self.port, read_only=True)
        self.zk.start()
    def getHost(self):
        return self.host
    def setHost(self,h):
        self.host = h
    def getPort(self):
        return self.port
    def setPort(self,p):
        self.port = p
    def getBrokerIDs(self):
        return self.zk.get_children("/brokers/ids")


if __name__ == "__main__":
    z = Zoo(host="localhost",port="2181")
    print(z.getBrokerIDs())

