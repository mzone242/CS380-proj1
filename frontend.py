from threading import Condition, Lock, RLock, Thread
from datetime import datetime, timedelta
from random import shuffle, choice
from time import sleep
import xmlrpc.client
import xmlrpc.server
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
from concurrent.futures import ThreadPoolExecutor, as_completed

lockedKeys = set()
keyMonitors = dict() # maps key to RWMonitor
keyMonitorLock = Lock()
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

    def __init__(self):
        self.t = Thread(target=self.heartbeat)
        self.t.start()

    # full write
    def put(self, key, value):
        global writeId

        def sendPut(serverId, key, value, writeId):
            try:
                with kvsServers[serverId][1]:
                    proxy = kvsServers[serverId][0]
                    response = proxy.put(key, value, writeId)
                    if response == "On it, boss":
                        serverTimestamps[serverId] = datetime.now()
            except xmlrpc.client.Fault as e:
                response = "Frontend failed on put."
            except Exception as e:
                # declare dead
                response =  "Timeout on put."
            return (serverId, response)

        def sendLog(serverId):
            try:
                with kvsServers[serverId][1]:
                    proxy = kvsServers[serverId][0]
                    response = proxy.processLog(log)
                    if response == "You got it, boss":
                        serverTimestamps[serverId] = datetime.now()
            except xmlrpc.client.Fault as e:
                response = "Frontend failed on log send."
            except Exception as e:
                # declare dead
                response = "Timeout on log send."
            return (serverId, response)

        
        # check for no servers
        if not len(kvsServers.keys()):
            return "ERR_NOSERVERS"
       
        # check if this key is new: if so, create monitor for it
        with keyMonitorLock:
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
                elif response == "Timeout on put." and datetime.now() - serverTimestamps[serverId] >= timedelta(seconds=0.1):
                    print(response + " No heartbeat in the past 0.1 seconds. Removing serverId "+str(serverId)+" from list.")        
                    results[serverId] = "Put timeout and no heartbeat; removing."  
                    kvsServers.pop(serverId, None)

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
                elif response == "Timeout on log send." and datetime.now() - serverTimestamps[serverId] >= timedelta(seconds=0.1):
                    print(response + " No heartbeat in the past 0.1 seconds. Removing serverId "+str(serverId)+" from list.")
                    results[serverId] = "Log timeout and no heartbeat; removing."   
                    kvsServers.pop(serverId, None)


            with writeIdLock:
                log.remove((thisWriteId, thisKey, thisValue))

            keyMonitor.writers -= 1
            if keyMonitor.waitingReaders == 0:
                keyMonitor.writeCV.notify()
            else:
                with keyMonitor.readCV:
                    keyMonitor.readCV.notify_all()

        return str(results)

    # read
    def get(self, key):
        response = str(key) + ":ERR_KEY"

        if key not in keyMonitors.keys():
            return response

        # beginRead
        keyMonitor = keyMonitors[key]
        with keyMonitor.readCV:
            keyMonitor.waitingReaders += 1
            if keyMonitor.writers > 0:
                keyMonitor.readCV.wait()
            keyMonitor.waitingReaders -= 1
            keyMonitor.readers += 1

        while(kvsServers):
            serverId = choice(list(kvsServers.keys()))
            # serverId = 0
            try:
                with kvsServers[serverId][1]:
                    proxy = kvsServers[serverId][0]
                    response = proxy.get(key)
                break
            except Exception as e:
                if datetime.now() - serverTimestamps[serverId] >= timedelta(seconds = 0.1):
                    # print("Server %d timeout on get and no heartbeat in the past 0.1 seconds. Removing." % serverId)
                    response = "Server "+str(serverId)+" timeout on get and no heartbeat in the past 0.1 seconds. Removing."
                    kvsServers.pop(serverId, None)

        # endRead
        with keyMonitor.readCV:
            keyMonitor.readers -= 1
            if keyMonitor.readers == 0 and keyMonitor.writers > 0:
                with keyMonitor.writeCV:
                    keyMonitor.writeCV.notify()

        return response
        

    def heartbeat(self):
        def sendHeartbeat(serverId):
            try:
                with kvsServers[serverId][1]:
                    proxy = kvsServers[serverId][0]
                    response = proxy.heartbeat()
                    if response == "I'm here for you, boss":
                        serverTimestamps[serverId] = datetime.now()
            except xmlrpc.client.Fault as e:
                response = "Frontend failed on heartbeat."
            except Exception as e:
                # declare dead
                response = "Timeout on heartbeat."
            return (serverId, response)

        while True:
            results = dict()
            with ThreadPoolExecutor() as executor:
                commands = {executor.submit(sendHeartbeat, serverId) for serverId, _ in kvsServers.items()}
                for future in as_completed(commands):
                    serverId, response = future.result()
                    results[serverId] = response

            for serverId, response in results.items():
                if response == "Frontend failed on heartbeat.":
                    print(response + " Time to panic.")
                elif response == "Timeout on heartbeat." and datetime.now() - serverTimestamps[serverId] >= timedelta(seconds=0.1):
                    print(response + " No put/get response in the past 0.1 seconds. Removing serverId "+str(serverId)+" from list.")
                    results[serverId] = "No recorded response in the past 0.1 seconds. Removing server."
                    kvsServers.pop(serverId, None)
            sleep(0.05)

        # unreachable
        return "Results of this heartbeat: " + str(results) + " and current timestamps after this heartbeat: " + str(serverTimestamps)


    ## printKVPairs: This function routes requests to servers
    ## matched with the given serverIds.
    def printKVPairs(self, serverId):
        if serverId not in kvsServers:
            return "ERR_NOEXIST"
        with kvsServers[serverId][1]: 
            return kvsServers[serverId][0].printKVPairs()

    ## addServer: This function registers a new server with the
    ## serverId to the cluster membership.
    def addServer(self, serverId):
        with writeIdLock:
            oldServerList = list(kvsServers.keys())
            responses = []
            serverTimestamps[serverId] = datetime.now()
            if oldServerList:
                shuffle(oldServerList)
                proxy_new = TimeoutServerProxy(baseAddr + str(baseServerPort + serverId))
                for server in oldServerList:
                    try:
                        proxy_old = kvsServers[server][0]
                        with kvsServers[server][1]:
                            kvPairs = proxy_old.printKVPairs()
                        responses.append(kvPairs)
                        responses.append(proxy_new.addKVPairs(kvPairs))
                        responses.append(proxy_new.updateWriteCtr(writeId))
                        if log:
                            responses.append(proxy_new.processLog(log))
                        kvsServers[serverId] = (proxy_new, Lock())
                        return str(responses)

                    except Exception as e:
                        pass
            
            kvsServers[serverId] = (TimeoutServerProxy(baseAddr + str(baseServerPort + serverId)), Lock())
            return str(kvsServers.keys()) + " " + str(responses)

    ## listServer: This function prints out a list of servers that
    ## are currently active/alive inside the cluster.
    def listServer(self):
        serverList = list(kvsServers.keys())
        if len(serverList): 
            return str(serverList)[1:-1]
        return "ERR_NOSERVERS"

    ## shutdownServer: This function routes the shutdown request to
    ## a server matched with the specified serverId to let the corresponding
    ## server terminate normally.
    def shutdownServer(self, serverId):
        if serverId not in kvsServers:
            return "ERR_NOEXIST"
        with kvsServers[serverId][1]:
            result = kvsServers[serverId][0].shutdownServer()
        kvsServers.pop(serverId, None)
        return result

server = SimpleThreadedXMLRPCServer(("localhost", 8001))
server.register_instance(FrontendRPCServer())

server.serve_forever()