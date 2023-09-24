import threading
import xmlrpc.client
import xmlrpc.server
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
from concurrent.futures import ThreadPoolExecutor, as_completed

serverTimestamps = dict()
kvsServers = dict()
baseAddr = "http://localhost:"
baseServerPort = 9000
writeId = 0

class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
        pass

class FrontendRPCServer:
    # full write

    def putListen(proxy, command):
        try:
            proxy = xmlrpc.client.ServerProxy("address of server", timeout=threshold)

            response = proxy.execute_command()
        except xmlrpc.client.Fault as e:
            print("frontend fault is: ", e)
        except Exception as e:
            print("exception is: ", e)
        

    def put(self, key, value):
        # serverId = key % len(kvsServers)

        # spawn one put thread per server, block til all servers ACK or timeout
        # lock key and add to list of server : key

        # how do we submit this command to multiple servers?
        # remember that you can only listen on a port once--so we have this thread listen on a port and then spawn other threads as needed


        with ThreadPoolExecutor() as executor:
            commands = {executor.submit(server.put) for _ in range(len(kvsServers))}
            for future in as_completed(commands):
                response = future.result()

        # serverId = 
        # returnVal = kvsServers[serverId].put(key, value, writeId)

        # if timeout and heartbeat not recorded in a while, declare dead
        # remember that timeout should be pretty long
        # if now - timeout > threshold and now - heartbeat > threshold: dead

        # if any server says they have gaps: send log and wait for ACK
        # if 

        # if all ACKs: success, unlock keys and return to client

        return returnVal

    # read
    def get(self, key):
        serverId = key % len(kvsServers)
        return kvsServers[serverId].get(key)

    def printKVPairs(self, serverId):
        return kvsServers[serverId].printKVPairs()

    def addServer(self, serverId):
        kvsServers[serverId] = xmlrpc.client.ServerProxy(baseAddr + str(baseServerPort + serverId))
        return "Success"

    def listServer(self):
        serverList = []
        for serverId, rpcHandle in kvsServers.items():
            serverList.append(serverId)
        return serverList

    def shutdownServer(self, serverId):
        result = kvsServers[serverId].shutdownServer()
        kvsServers.pop(serverId)
        return result

server = SimpleThreadedXMLRPCServer(("localhost", 8001))
server.register_instance(FrontendRPCServer())

server.serve_forever()
