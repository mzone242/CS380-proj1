from threading import Condition
from threading import RLock
from threading import Lock
from datetime import datetime
from random import shuffle
import xmlrpc.client
import xmlrpc.server
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
from concurrent.futures import ThreadPoolExecutor, as_completed

lockedKeys = set()
keyMonitors = dict() # maps key to RWMonitor
log = [] # tuple of (writeId, key, value)
serverTimestamps = dict()
kvsServers = dict()
baseAddr = "http://localhost:"
baseServerPort = 9000
writeId = 0
writeIdLock = Lock()

class RWMonitor:
    def __init__(self):
        self.readCV = Condition()
        self.writeCV = Condition()
        self.readers, self.writers, self.waitingReaders = 0, 0, 0

class TimeoutTransport(xmlrpc.client.Transport):

    def __init__(self, timeout, use_datetime=0):
        self.timeout = timeout
        xmlrpc.client.Transport.__init__(self, use_datetime)

    def make_connection(self, host):
        connection = xmlrpc.client.Transport.make_connection(self, host)
        connection.timeout = self.timeout
        return connection

class TimeoutServerProxy(xmlrpc.client.ServerProxy):

    def __init__(self, uri, timeout=0.1, transport=None, encoding=None, verbose=0, allow_none=0, use_datetime=0):
        t = TimeoutTransport(timeout)
        xmlrpc.client.ServerProxy.__init__(self, uri, t, encoding, verbose, allow_none, use_datetime)

class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
        pass

class FrontendRPCServer:
    # full write
    def put(self, key, value):
        global writeId

        def sendPut(serverId, key, value, writeId):
            try:
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

        def sendLog(serverId):
            try:
                proxy = TimeoutServerProxy(baseAddr + str(baseServerPort + serverId))
                # TODO: add this function to server
                response = proxy.processLog(log)
                if response == "You got it, boss":
                    serverTimestamps[serverId] = datetime.now()
            except xmlrpc.client.Fault as e:
                response = "Frontend failed on log send."
            except Exception as e:
                # declare dead
                response = "Timeout on log send."
            return (serverId, response)

       
        # check if this key is new: if so, create monitor for it
        if key not in keyMonitors.keys():
            keyMonitors[key] = RWMonitor()
        keyMonitor = keyMonitors[key]

        with keyMonitor.writeCV:
            keyMonitor.writers += 1
            while keyMonitor.readers > 0:
                keyMonitor.writeCV.wait()
            # spawn one put thread per server, block til all servers ACK/timeout
            # lock key and add to list of server : key
            results = dict()
            with writeIdLock:
                writeId += 1
                thisWriteId, thisKey, thisValue = writeId, key, value
                log.append((thisWriteId, thisKey, thisValue))

            with ThreadPoolExecutor() as executor:
                commands = {executor.submit(sendPut, serverId, key, value, writeId) for serverId, _ in kvsServers.items()}
                for future in as_completed(commands):
                    serverId, response = future.result()
                    results[serverId] = response

            # if timeout on put and heartbeat not recorded in a while, it's dead
            for serverId, response in results.items():
                if response == "Frontend failed on put.":
                    print(response + " Time to panic.")
                elif response == "Timeout on put." and datetime.now() - serverTimestamps[serverId] >= datetime.timedelta(seconds=0.1):
                    print(response + " No heartbeat in the past 0.1 seconds. Removing serverId "+str(serverId)+" from list.")        
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
                    results[serverId] = "Frontend failed on log send; panicking."
                elif response == "Timeout on log send." and datetime.now() - serverTimestamps[serverId] >= datetime.timedelta(seconds=0.1):
                    print(response + " No heartbeat in the past 0.1 seconds. Removing serverId "+str(serverId)+" from list.")
                    results[serverId] = "Log timeout and no heartbeat; removing."   
                    kvsServers.pop(serverId)


            with writeIdLock:
                log.remove((thisWriteId, thisKey, thisValue))

            keyMonitor.writers -= 1
            if keyMonitor.waitingReaders == 0:
                keyMonitor.writeCV.notify()
            else:
                keyMonitor.readCV.notify_all()

        return str(results)

    # read
    def get(self, key):
        serverList = list(kvsServers.keys())
        shuffle(serverList)
        response = str(key) + ":ERR_KEY"

        if key not in keyMonitors.key():
            return response

        # beginRead
        keyMonitor = keyMonitors[key]
        with keyMonitor.readCV:
            keyMonitor.waitingReaders += 1
            if keyMonitor.writers > 0:
                keyMonitor.readCV.wait()
            keyMonitor.waitingReaders -= 1
            keyMonitor.readers += 1

        for serverId in serverList:
            try:
                proxy = TimeoutServerProxy(baseAddr + str(baseServerPort + serverId))
                response = proxy.get(key)
            except Exception as e:
                if datetime.now() - serverTimestamps[serverId] >= datetime.timedelta(seconds = 0.1):
                    # print("Server %d timeout on get and no heartbeat in the past 0.1 seconds. Removing." % serverId)
                    response = "Server "+str(serverId)+" timeout on get and no heartbeat in the past 0.1 seconds. Removing."
                    kvsServers.pop(serverId)

        # endRead
        with keyMonitor.readCV:
            keyMonitor.readers -= 1
            if keyMonitor.readers == 0:
                keyMonitor.writeCV.notify()

        return response
        

    def heartbeat(self):
        def sendHeartbeat(serverId):
            try:
                proxy = TimeoutServerProxy(baseAddr + str(baseServerPort + serverId))
                response = proxy.heartbeat()
                if response == "I'm here for you, boss":
                    serverTimestamps[serverId] = datetime.now()
            except xmlrpc.client.Fault as e:
                response = "Frontend failed on heartbeat."
            except Exception as e:
                # declare dead
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
            elif response == "Timeout on heartbeat." and datetime.now() - serverTimestamps[serverId] >= datetime.timedelta(seconds=0.1):
                print(response + " No put/get response in the past 0.1 seconds. Removing serverId "+str(serverId)+" from list.")
                results[serverId] = "No recorded response in the past 0.1 seconds. Removing server."

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
                    serverTimestamps[serverId] = datetime.now()
                    return str(responses)

                except Exception as e:
                    pass
                    # if datetime.now() - serverTimestamps[sID] >= datetime.timedelta(seconds=0.1):
                    #     # print("Server %d timeout on get and no heartbeat in the past 0.1 seconds. Removing." % serverId)
                    #     response = "Server "+str(sID)+" timeout on get and no heartbeat in the past 0.1 seconds. Removing."
                    #     kvsServers.pop(sID)
        
        return str(kvsServers.keys()) + " " + str(responses)

    ## listServer: This function prints out a list of servers that
    ## are currently active/alive inside the cluster.
    def listServer(self):
        # serverList = []
        # for serverId, rpcHandle in kvsServers.items():
        #     serverList.append(serverId)
        # st = str(serverList)[1:-1]
        if len(serverList): 
            return kvsServers.keys()
        return "ERR_NOSERVERS"

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