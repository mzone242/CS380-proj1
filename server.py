import argparse
import xmlrpc.client
import xmlrpc.server
from xmlrpc.server import SimpleXMLRPCServer

serverId = 0
basePort = 9000

kvStore = dict()
keyDNE = "ERR_KEY"
writeCtr = 0 # most recent writeId that we've seen; used to check for gaps

# class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
#         pass

class KVSRPCServer():
    quit = False

    def __init__(self, port):
        self.server = SimpleXMLRPCServer(("localhost", port))
        self.server.register_instance(self)
        while not self.quit:
            self.server.handle_request()
        exit()


    # if receiving a sequential writeId, commit immediately
    # otherwise just drop msg and tell frontend of discrepancy to receive log
    # then execute log in order
    def put(self, key, value): #, writeId):
        kvStore[key] = value
        return "On it boss"
        # if writeID == writeCtr + 1:
        #     kvStore[key] = value
        #     return "On it boss"
        #     # return "[Server " + str(serverId) + "] Receive a put request: " + "Key = " + str(key) + ", Val = " + str(value)
        # else:
        #     # need to alert frontend to send 
        #     return "Need writeId" + str(writeCtr + 1)

    def get(self, key):
        # return in format k:v
        return str(key) + ":" + str(kvStore.get(key, keyDNE))
        # return "[Server " + str(serverId) + "] Receive a get request: " + "Key = " + str(key)

    def printKVPairs(self):
        # return in format k1:v1, k2:v2, k3:v3, ...
        return "".join("{}:{}, ".format(k, v) for k, v in kvStore.items())[:-2]
        # return "[Server " + str(serverId) + "] Receive a request printing all KV pairs stored in this server"

    def shutdownServer(self):
        # clean up threads?
        self.quit = True
        return "[Server " + str(serverId) + "] Shutting down"
        # return "[Server " + str(serverId) + "] Receive a request for a normal shutdown"

    def heartbeat(self):
        return "Alive"

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = '''To be added.''')

    parser.add_argument('-i', '--id', nargs=1, type=int, metavar='I',
                        help='Server id (required)', dest='serverId', required=True)

    args = parser.parse_args()

    serverId = args.serverId[0]

    KVSRPCServer(basePort + serverId)

    # server = xmlrpc.server.SimpleXMLRPCServer(("localhost", basePort + serverId))
    # server.register_instance(KVSRPCServer())

    # server.serve_forever()
