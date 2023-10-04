from datetime import datetime
from threading import Lock
from random import shuffle
import xmlrpc.client
import xmlrpc.server
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
from concurrent.futures import ThreadPoolExecutor, as_completed

lockedKeys = set()
keyLocks = dict()
log = [] # tuple of (writeId, key, value)
# timestampLock = Lock()
serverTimestamps = dict()
kvsServers = dict()
baseAddr = "http://localhost:"
baseServerPort = 9000
writeId = 0
writeIdLock = Lock()
serverLocks = dict()

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
        # threads = []
        # for serverId, server in kvsServers.items():
        #     thread = threading.Thread(target = putListen, args = (baseAddr + str(baseServerPort + serverId), (key, value)))
        #     threads.append(thread)
        #     thread.start()

        # for t in threads:
        #     t.join()

        def sendPut(serverId, key, value, writeId):
            try:
                # lockedKeys.add(key)
                proxy = TimeoutServerProxy(baseAddr + str(baseServerPort + serverId))
                response = proxy.put(key, value, writeId)
                if response == "On it, boss":
                    serverTimestamps[serverId] = datetime.now()
            except xmlrpc.client.Fault as e:
                raise Exception(serverId, e, "Frontend failed on put.")
            except Exception as e:
                # declare dead
                raise Exception(serverId, e, "Timeout on put.")
            return (serverId, response)
        # serverId = key % len(kvsServers)

        def sendLog(serverId):
            try:
                proxy = TimeoutServerProxy(baseAddr + str(baseServerPort + serverId))
                # TODO: add this function to server
                response = proxy.processLog(log)
                if response == "You got it, boss":
                    serverTimestamps[serverId] = datetime.now()
            except xmlrpc.client.Fault as e:
                # raise Exception(serverId, e, "Frontend failed on log send.")
                response = "Frontend failed on log send."
            except Exception as e:
                # declare dead
                # raise Exception(serverId, e, "Timeout on log send.")
                response = "Timeout on log send."
            return (serverId, response)

       
        # check if this key is new: if so, create lock for it
        if key not in keyLocks.keys():
            keyLocks[key] = Lock()
        keyLock = keyLocks[key]
        with keyLock:
            # spawn one put thread per server, block til all servers ACK/timeout
            # lock key and add to list of server : key
            results = dict()
            with writeIdLock:
                writeId += 1
                thisWriteId, thisKey, thisValue = writeId, key, value
                log.append((thisWriteId, thisKey, thisValue))
            
            # with lock():
            #     pass

            with ThreadPoolExecutor() as executor:
                commands = {executor.submit(sendPut, serverId, key, value, writeId) for serverId, _ in kvsServers.items()}
                for future in as_completed(commands):
                    serverId, response = future.result()
                    results[serverId] = response

            # if timeout on put and heartbeat not recorded in a while, it's dead
            for serverId, response in results.items():
                if response == "Frontend failed on put.":
                    print(response + " Time to panic.")
                elif response == "Timeout on put." and datetime.now() - serverTimestamps[serverId] >= datetime.timedelta(seconds=5):
                    print(response + " No heartbeat in the past 5 seconds. Removing serverId "+str(serverId)+" from list.")        
                    results[serverId] = "Put timeout and no heartbeat; removing."  
                    kvsServers.pop(serverId)


            # if any server says they have gaps: send log and wait for ACK
            with ThreadPoolExecutor() as executor:
                commands = {executor.submit(sendLog, serverId) for serverId, response in results.items() if response == "No can do, boss"}
                for future in as_completed(commands):
                    serverId, response = future.result()
                    results[serverId] = response

            # again, see if there's any timeouts on log send, and if so, remove
            for serverId, response in results.items():
                if response == "Frontend failed on log send.":
                    print(response + " Time to panic.")
                    # results[serverId] = "Frontend failed on log send; panicking."
                elif response == "Timeout on log send." and datetime.now() - serverTimestamps[serverId] >= datetime.timedelta(seconds=5):
                    print(response + " No heartbeat in the past 5 seconds. Removing serverId "+str(serverId)+" from list.")
                    results[serverId] = "Log timeout and no heartbeat; removing."   
                    kvsServers.pop(serverId)

            # last action before releasing keyLock
            log.remove((thisWriteId, thisKey, thisValue))

        return str(results)

    # read
    def get(self, key):
        # send read to first server w/ this key unlocked
        # condvar instead of lock here?
        serverList = list(kvsServers.keys())
        shuffle(serverList)
        for serverId in serverList:
            # with serverLocks[serverId]:
            try:
                proxy = TimeoutServerProxy(baseAddr + str(baseServerPort + serverId))
                response = proxy.get(key)
                return str(response)
            except Exception as e:
                if datetime.now() - serverTimestamps[serverId] >= datetime.timedelta(seconds=5):
                    # print("Server %d timeout on get and no heartbeat in the past 5 seconds. Removing." % serverId)
                    response = "Server "+str(serverId)+" timeout on get and no heartbeat in the past 5 seconds. Removing."
                    kvsServers.pop(serverId)
        return key + ":ERR_KEY"
        

    def heartbeat(self):
        def sendHeartbeat(serverId):
            try:
                proxy = TimeoutServerProxy(baseAddr + str(baseServerPort + serverId))
                # TODO: add this function to server
                response = proxy.heartbeat()
                if response == "I'm here for you, boss":
                    serverTimestamps[serverId] = datetime.now()
            except xmlrpc.client.Fault as e:
                # raise Exception(serverId, e, "Frontend failed on log send.")
                response = "Frontend failed on heartbeat."
            except Exception as e:
                # declare dead
                # raise Exception(serverId, e, "Timeout on log send.")
                response = "Timeout on heartbeat."
            return (serverId, response)

        results = dict()
        with ThreadPoolExecutor() as executor:
            commands = {executor.submit(sendHeartbeat, serverId) for serverId, _ in kvsServers.items()}
            for future in as_completed(commands):
                serverId, response = future.result()
                results[serverId] = response

        for serverId, response in results.items():
            if response == "Frontend failed on heartbeat.":
                print(response + " Time to panic.")
            elif response == "Timeout on heartbeat." and datetime.now() - serverTimestamps[serverId] >= datetime.timedelta(seconds=5):
                print(response + " No put/get response in the past 5 seconds. Removing serverId "+str(serverId)+" from list.")
                results[serverId] = "No recorded response in the past 5 seconds. Removing server."

        return "Results of this heartbeat: " + str(results) + " and current timestamps after this heartbeat: " + str(serverTimestamps)


    ## printKVPairs: This function routes requests to servers
    ## matched with the given serverIds.
    def printKVPairs(self, serverId):
        if serverId not in kvsServers:
            return "ERR_NOEXIST"
        return kvsServers[serverId].printKVPairs()

    ## addServer: This function registers a new server with the
    ## serverId to the cluster membership.
    def addServer(self, serverId):
        # with lock():
        oldServerList = list(kvsServers.keys())
        kvsServers[serverId] = xmlrpc.client.ServerProxy(baseAddr + str(baseServerPort + serverId))
        responses = []
        if oldServerList:
            shuffle(oldServerList)
            for sID in oldServerList:
                try:
                    proxy = TimeoutServerProxy(baseAddr + str(baseServerPort + sID))
                    kvPairs = proxy.printKVPairs()
                    responses.append(kvPairs)
                    proxy = TimeoutServerProxy(baseAddr + str(baseServerPort + serverId))
                    responses.append(proxy.addKVPairs(kvPairs))
                    responses.append(proxy.updateWriteCtr(writeId))
                    if log:
                        responses.append(proxy.processLog(log))
                    return str(responses)

                except Exception as e:
                    pass
                    # if datetime.now() - serverTimestamps[sID] >= datetime.timedelta(seconds=5):
                    #     # print("Server %d timeout on get and no heartbeat in the past 5 seconds. Removing." % serverId)
                    #     response = "Server "+str(sID)+" timeout on get and no heartbeat in the past 5 seconds. Removing."
                    #     kvsServers.pop(sID)
        
        return str(kvsServers.keys()) + " " + str(responses)

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