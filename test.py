import argparse
import xmlrpc.client
from random import randint
from threading import Thread

basePort = 7000

class Client:
    def __init__(self, clientId=0):
        self.client = xmlrpc.client.ServerProxy("http://localhost:" + str(basePort + clientId))

    def put(self, key, value):
        return self.client.put(key, value)

    def get(self, key):
        return self.client.get(key)

def read(client, reads):
    for _ in range(0, reads):
        print(client.get(randint(0, 100)))


client1 = Client(0)
for i in range(0, 100):
    print(client1.put(i, i))

# Start all threads. 
threads = []
for n in range(3):
    t = Thread(target=read, args=(Client(n), 500,))
    t.start()
    threads.append(t)

# Wait all threads to finish.
for t in threads:
    t.join()
