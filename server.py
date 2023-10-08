import argparse
import xmlrpc.client
import xmlrpc.server
from xmlrpc.server import SimpleXMLRPCServer

serverId = 0
basePort = 9000

kvStore = dict()
keyDNE = "ERR_KEY"
writeCtr = 0 # most recent writeId that we've seen; used to check for gaps

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
    def put(self, key, value, writeId):
        global writeCtr
        if writeId == writeCtr + 1:
            kvStore[key] = value
            writeCtr += 1
            return "ACK"
        else:
            # need to alert frontend to send log
            return "NACK"

    # return in format k:v
    def get(self, key):
        return str(key) + ":" + str(kvStore.get(key, keyDNE))

    # return in format k1:v1\nk2:v2\nk3:v3\n...
    def printKVPairs(self):
        return "".join("{}:{}\n".format(k, v) for k, v in kvStore.items())

    # stops handle_request() loop in the init function so server can gracefully exit
    def shutdownServer(self):
        self.quit = True
        return "[Server " + str(serverId) + "] Shutting down"

    # process write log from frontend
    def processLog(self, log):
        global writeCtr
        # update log entries
        for _, k, v in log:
            kvStore[int(k)] = int(v)
        # update our writeID
        writeCtr = log[-1][0]
        return "ACK"

    # used on server startup to update kvStore with that of another server
    def addKVPairs(self, kvPairs):
        if ":" in kvPairs:
            kvList = kvPairs.split()
            for pair in kvList:
                k, v = pair.split(":")
                kvStore[int(k)] = int(v)
        return "ACK"

    # sets writeCtr on server startup
    def updateWriteCtr(self, writeId):
        global writeCtr
        writeCtr = writeId
        return "ACK"

    # heartbeat so frontend knows server is alive
    def heartbeat(self):
        return "Alive"

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = '''To be added.''')

    parser.add_argument('-i', '--id', nargs=1, type=int, metavar='I',
                        help='Server id (required)', dest='serverId', required=True)

    args = parser.parse_args()

    serverId = args.serverId[0]

    KVSRPCServer(basePort + serverId)
