from threading import Condition, Lock, RLock, Thread
from datetime import datetime, timedelta
from random import shuffle, choice
from time import sleep
import xmlrpc.client
import xmlrpc.server
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
from concurrent.futures import ThreadPoolExecutor, as_completed

keyMonitors = dict()        # maps key:RWMonitor for every key in kvstore
keyMonitorLock = Lock()     # protecting adding a RWMonitor to kvsServers
serverTimestamps = dict()   # maps serverId:datetime of most recent response
kvsServers = dict()         # maps serverId:(TimeoutServerProxy, proxyLock)
writeId = 0                 # monotonically increasing with each new write
log = []                    # tuple of (writeId, key, value)
writeIdLogLock = Lock()     # used to protect incrementing writeId and modifying
baseAddr = "http://localhost:"
baseServerPort = 9000

class RWMonitor:
    def __init__(self):
        self.readCV = Condition()
        self.writeCV = Condition()
        self.readers, self.writers, self.waitingReaders = 0, 0, 0

# custom serverproxy with included timeout
class TimeoutTransport(xmlrpc.client.Transport):

    def __init__(self, timeout, use_datetime=0):
        self.timeout = timeout
        xmlrpc.client.Transport.__init__(self, use_datetime)

    def make_connection(self, host):
        connection = xmlrpc.client.Transport.make_connection(self, host)
        connection.timeout = self.timeout
        return connection

class TimeoutServerProxy(xmlrpc.client.ServerProxy):

    def __init__(self, uri, timeout=0.5, transport=None, encoding=None, verbose=0, allow_none=0, use_datetime=0):
        t = TimeoutTransport(timeout)
        xmlrpc.client.ServerProxy.__init__(self, uri, t, encoding, verbose, allow_none, use_datetime)

class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
        pass

class FrontendRPCServer:

    def __init__(self):
        # start heartbeat
        self.t = Thread(target=self.heartbeat)
        self.t.start()

    # full write
    def put(self, key, value):
        global writeId

        # threaded function: each spawned thread will send a put request to a server
        def sendPut(serverId, key, value, writeId):
            try:
                with kvsServers[serverId][1]:
                    proxy = kvsServers[serverId][0]
                    response = proxy.put(key, value, writeId)
                    if response == "ACK":
                        serverTimestamps[serverId] = datetime.now()
            except xmlrpc.client.Fault as e:
                response = "Frontend failed on put."
            except Exception as e:
                # declare dead
                response =  "Timeout on put."
            return (serverId, response)

        # threaded function: each spawned thread will send the FE's log to a server
        def sendLog(serverId):
            try:
                with kvsServers[serverId][1]:
                    proxy = kvsServers[serverId][0]
                    response = proxy.processLog(log)
                    if response == "ACK":
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

        # entering critical section; first part of readers-writers
        with keyMonitor.writeCV:
            keyMonitor.writers += 1
            while keyMonitor.readers > 0:
                keyMonitor.writeCV.wait()

            # increment writeId and add this put operation to log
            results = dict()
            with writeIdLogLock:
                writeId += 1
                thisWriteId, thisKey, thisValue = writeId, key, value
                log.append((thisWriteId, thisKey, thisValue))

            # spawn one put thread per server, wait til all servers ACK/NACK/timeout
            with ThreadPoolExecutor() as executor:
                commands = {executor.submit(sendPut, serverId, key, value, writeId) for serverId, _ in kvsServers.items()}
                for future in as_completed(commands):
                    serverId, response = future.result()
                    results[serverId] = response

            # if timeout on put and no other response from that server in a while, it's dead
            for serverId, response in results.items():
                if response == "Frontend failed on put.":
                    # time to panic, unrecoverable if there's actually an issue
                    pass
                elif response == "Timeout on put." and datetime.now() - serverTimestamps[serverId] >= timedelta(seconds=0.5):
                    # no response in past 0.5 seconds, remove server
                    kvsServers.pop(serverId, None)

            # if any server NACKS, meaning they're missing puts: send log and wait for ACK/timeout
            with ThreadPoolExecutor() as executor:
                commands = {executor.submit(sendLog, serverId) for serverId, response in results.items() if response == "NACK"}
                for future in as_completed(commands):
                    serverId, response = future.result()
                    results[serverId] = response

            # again, see if there's any timeouts on log send, and if so, remove
            for serverId, response in results.items():
                if response == "Frontend failed on log send.":
                    # time to panic, unrecoverable if there's actually an issue
                    pass
                elif response == "Timeout on log send." and datetime.now() - serverTimestamps[serverId] >= timedelta(seconds=0.5):
                    # no response in past 0.5 seconds, remove server
                    kvsServers.pop(serverId, None)

            # if all servers have responded to this put or timed out, then we're done.
            # remove write from log and commence wrap-up
            with writeIdLogLock:
                log.remove((thisWriteId, thisKey, thisValue))

            # ending portion of readers-writers, for writers
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

        # beginRead function in readers-writers
        keyMonitor = keyMonitors[key]
        with keyMonitor.readCV:
            keyMonitor.waitingReaders += 1
            if keyMonitor.writers > 0:
                keyMonitor.readCV.wait()
            keyMonitor.waitingReaders -= 1
            keyMonitor.readers += 1

        # find a random server to send get() request to
        while(kvsServers):
            serverId = choice(list(kvsServers.keys()))
            # serverId = 0
            try:
                with kvsServers[serverId][1]:
                    proxy = kvsServers[serverId][0]
                    response = proxy.get(key)
                break
            except Exception as e:
                if datetime.now() - serverTimestamps[serverId] >= timedelta(seconds = 0.5):
                    # no response in past 0.5 seconds, remove server
                    kvsServers.pop(serverId, None)

        # endRead function in readers-writers
        with keyMonitor.readCV:
            keyMonitor.readers -= 1
            if keyMonitor.readers == 0 and keyMonitor.writers > 0:
                with keyMonitor.writeCV:
                    keyMonitor.writeCV.notify()

        return response
        

    def heartbeat(self):

        # threaded function: each spawned thread will send a heartbeat to a server
        def sendHeartbeat(serverId):
            try:
                with kvsServers[serverId][1]:
                    proxy = kvsServers[serverId][0]
                    response = proxy.heartbeat()
                    if response == "Alive":
                        serverTimestamps[serverId] = datetime.now()
            except xmlrpc.client.Fault as e:
                response = "Frontend failed on heartbeat."
            except Exception as e:
                # declare dead
                response = "Timeout on heartbeat."
            return (serverId, response)

        # heartbeat every 0.05 seconds
        while True:
            results = dict()
            with ThreadPoolExecutor() as executor:
                commands = {executor.submit(sendHeartbeat, serverId) for serverId, _ in kvsServers.items()}
                for future in as_completed(commands):
                    serverId, response = future.result()
                    results[serverId] = response

            for serverId, response in results.items():
                if response == "Frontend failed on heartbeat.":
                    # time to panic, unrecoverable if there's actually an issue
                    pass
                elif response == "Timeout on heartbeat." and datetime.now() - serverTimestamps[serverId] >= timedelta(seconds=0.5):
                    # no response in past 0.5 seconds, remove server
                    kvsServers.pop(serverId, None)
            sleep(0.05)
        # unreachable


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
        with writeIdLogLock:
            oldServerList = list(kvsServers.keys())
            serverTimestamps[serverId] = datetime.now()
            # if only server, no data to update
            if oldServerList:
                shuffle(oldServerList)
                proxy_new = TimeoutServerProxy(baseAddr + str(baseServerPort + serverId))
                for server in oldServerList:
                    try:
                        # update new server's kvStore from another server and current log entries
                        # also update server's writeCtr
                        proxy_old = kvsServers[server][0]
                        with kvsServers[server][1]:
                            kvPairs = proxy_old.printKVPairs()
                        proxy_new.addKVPairs(kvPairs)
                        proxy_new.updateWriteCtr(writeId)
                        if log:
                            proxy_new.processLog(log)
                        kvsServers[serverId] = (proxy_new, Lock())
                        return "Success"

                    except Exception as e:
                        # try next server, if the new server is the one failing we can't recover
                        pass
            
            kvsServers[serverId] = (TimeoutServerProxy(baseAddr + str(baseServerPort + serverId)), Lock())
            return "Success"

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