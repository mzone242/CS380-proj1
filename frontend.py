import threading
import xmlrpc.client
import xmlrpc.server
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
from concurrent.futures import ThreadPoolExecutor, as_completed

lockedServerKeyPairs = set()
log = [] # tuple of (writeId, key, value)
serverTimestamps = dict()
kvsServers = dict()
baseAddr = "http://localhost:"
baseServerPort = 9000
writeId = 0

class TimeoutTransport(xmlrpc.client.Transport):

    def __init__(self, timeout, use_datetime=0):
        self.timeout = timeout
        xmlrpc.client.Transport.__init__(self, use_datetime)

    def make_connection(self, host):
        connection = xmlrpc.client.Transport.make_connection(self, host)
        connection.timeout = self.timeout
        return connection

class TimeoutServerProxy(xmlrpc.client.ServerProxy):

    def __init__(self, uri, timeout=5, transport=None, encoding=None, verbose=0, allow_none=0, use_datetime=0):
        t = TimeoutTransport(timeout)
        xmlrpc.client.ServerProxy.__init__(self, uri, t, encoding, verbose, allow_none, use_datetime)

class TimeoutTransport(xmlrpc.client.Transport):

    def __init__(self, timeout, use_datetime=0):
        self.timeout = timeout
        xmlrpc.client.Transport.__init__(self, use_datetime)

    def make_connection(self, host):
        connection = xmlrpc.client.Transport.make_connection(self, host)
        connection.timeout = self.timeout
        return connection

class TimeoutServerProxy(xmlrpc.client.ServerProxy):

    def __init__(self, uri, timeout=5, transport=None, encoding=None, verbose=0, allow_none=0, use_datetime=0):
        t = TimeoutTransport(timeout)
        xmlrpc.client.ServerProxy.__init__(self, uri, t, encoding, verbose, allow_none, use_datetime)

class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
        pass

class FrontendRPCServer:
    # full write
    def put(self, key, value):
        global writeId

        '''first attempt using threading below, don't comment in'''
        # threads = []
        # for serverId, server in kvsServers.items():
        #     thread = threading.Thread(target = sendPut, args = (baseAddr + str(baseServerPort + serverId), (key, value)))
        #     threads.append(thread)
        #     thread.start()

        # for t in threads:
        #     t.join()

        def sendPut(serverId, key, value, writeId):
            try:
                lockedServerKeyPairs.add((serverId, key))
                proxy = TimeoutServerProxy(baseAddr + str(baseServerPort + serverId))
                response = proxy.put(key, value, writeId)
            except xmlrpc.client.Fault as e:
                raise Exception(serverId, e, "Frontend failed on put.")
            except Exception as e:
                # declare dead
                raise Exception(serverId, e, "Timeout on put.")
            return (serverId, response)

        def sendLog(serverId):
            try:
                proxy = TimeoutServerProxy(baseAddr + str(baseServerPort + serverId))
                # TODO: add this function to server
                response = proxy.processLog(log)
            except xmlrpc.client.Fault as e:
                raise Exception(serverId, e, "Frontend failed on log send.")
            except Exception as e:
                # declare dead
                raise Exception(serverId, e, "Timeout on log send.")
            return (serverId, response)

        # spawn one put thread per server, block til all servers ACK or timeout
        # lock key and add to list of server : key
        results = dict()
        writeId += 1
        log.append((writeId, key, value))
        with ThreadPoolExecutor() as executor:
            try:
                commands = {executor.submit(sendPut, serverId, key, value, writeId) for serverId, _ in kvsServers.items()}
                for future in as_completed(commands):
                    serverId, response = future.result()
                    results[serverId] = response
            # if timeout and heartbeat not recorded in a while, declare dead
            except Exception as e:
                print(e)
                serverId, exception, msg = e.args
                if str(msg) == "Timeout on put.":
                    print("Server %d timeout on put, removing." % serverId)
                    lockedServerKeyPairs.remove((serverId, key))
                    kvsServers.pop(serverId)

        # if any server says they have gaps: send log and wait for ACK
        with ThreadPoolExecutor() as executor:
            try:
                commands = {executor.submit(sendLog, serverId) for serverId, response in results if response == "NACK"}
            except Exception as e:
                print(e)
                serverId, exception, msg = e.args
                if str(msg) == "Timeout on log send.":
                    print("Server %d timeout on log send, removing." % serverId)
                    lockedServerKeyPairs.remove((serverId, key))
                    kvsServers.pop(serverId)
                
        # results now contains only serverIds who have succeeded
        # if all ACKs: success, unlock keys and return to client
        for serverId, _ in results.items():
            lockedServerKeyPairs.remove((serverId, key))

        return value

    # read
    def get(self, key):
        # send read to first server w/ this key unlocked
        for serverId in list(kvsServers.keys()):
            if (serverId, key) not in lockedServerKeyPairs:
                try:
                    proxy = TimeoutServerProxy(baseAddr + str(baseServerPort + serverId))
                    response = proxy.get(key)
                    return response
                except Exception as e:
                    print("Server %d timeout on get, removing." % serverId)
                    kvsServers.pop(serverId)
        return str(lockedServerKeyPairs)
    
        ''' boilerplate code below'''
        # serverId = key, "Timeout on log send." % len(kvsServers)
        # return kvsServers[serverId].get(key)

    ## printKVPairs: This function routes requests to servers
    ## matched with the given serverIds.
    def printKVPairs(self, serverId):
        if serverId not in kvsServers:
            return "ERR_NOEXIST"

        # for server, key in lockedServerKeyPairs:
            # if serverId == server:
                
        return kvsServers[serverId].printKVPairs()

    ## addServer: This function registers a new server with the
    ## serverId to the cluster membership.
    def addServer(self, serverId):
        kvsServers[serverId] = xmlrpc.client.ServerProxy(baseAddr + str(baseServerPort + serverId))
        return "Success"

    ## listServer: This function prints out a list of servers that
    ## are currently active/alive inside the cluster.
    def listServer(self):
        serverList = []
        for serverId, rpcHandle in kvsServers.items():
            serverList.append(serverId)
        if not len(serverList): return "ERR_NOSERVERS"
        return serverList

    ## shutdownServer: This function routes the shutdown request to
    ## a server matched with the specified serverId to let the corresponding
    ## server terminate normally.
    def shutdownServer(self, serverId):
        if serverId not in kvsServers:
            return "ERR_NOEXIST"
        result = kvsServers[serverId].shutdownServer()
        kvsServers.pop(serverId)
        return result

server = SimpleThreadedXMLRPCServer(("localhost", 8001))
server.register_instance(FrontendRPCServer())

server.serve_forever()