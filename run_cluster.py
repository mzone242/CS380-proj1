import argparse
import os
import subprocess

import time
import random
import xmlrpc.client

import concurrent.futures

from shared import util

baseAddr = "http://localhost:"
baseClientPort = 7000
baseFrontendPort = 8001
baseServerPort = 9000

clientUID = 0
serverUID = 0

frontend = None
clientList = dict()

def add_nodes(k8s_client, k8s_apps_client, node_type, num_nodes, prefix=None):
    global clientUID
    global serverUID

    for i in range(0, num_nodes):
        if node_type == 'server':
            server_spec = util.load_yaml('yaml/pods/server-pod.yml', prefix)
            env = server_spec['spec']['containers'][0]['env']
            util.replace_yaml_val(env, 'SERVER_ID', str(serverUID))
            server_spec['metadata']['name'] = 'server-pod-%d' % serverUID
            server_spec['metadata']['labels']['role'] = 'server-%d' % serverUID
            k8s_client.create_namespaced_pod(namespace=util.NAMESPACE, body=server_spec)
            util.check_wait_pod_status(k8s_client, 'role=server-%d' % serverUID, 'Running')
            result = frontend.addServer(serverUID)
            serverUID += 1
        elif node_type == 'client':
            client_spec = util.load_yaml('yaml/pods/client-pod.yml', prefix)
            env = client_spec['spec']['containers'][0]['env']
            util.replace_yaml_val(env, 'CLIENT_ID', str(clientUID))
            client_spec['metadata']['name'] = 'client-pod-%d' % clientUID
            client_spec['metadata']['labels']['role'] = 'client-%d' % clientUID
            k8s_client.create_namespaced_pod(namespace=util.NAMESPACE, body=client_spec)
            util.check_wait_pod_status(k8s_client, 'role=client-%d' % clientUID, 'Running')
            clientList[clientUID] = xmlrpc.client.ServerProxy(baseAddr + str(baseClientPort + clientUID))
            clientUID += 1
        else:
            print("Unknown pod type")
            exit()

def remove_node(k8s_client, k8s_apps_client, node_type, node_id):
    name = node_type + '-pod-%d' % node_id
    selector = 'role=' + node_type + '-%d' % node_id
    k8s_client.delete_namespaced_pod(name, namespace=util.NAMESPACE)
    util.check_wait_pod_status(k8s_client, selector, 'Terminating')

def addClient(k8s_client, k8s_apps_client, prefix):
    add_nodes(k8s_client, k8s_apps_client, 'client', 1, prefix)

def addServer(k8s_client, k8s_apps_client, prefix):
    add_nodes(k8s_client, k8s_apps_client, 'server', 1, prefix)

def listServer():
    result = frontend.listServer()
    print(result)

def killServer(k8s_client, k8s_apps_client, serverId):
    remove_node(k8s_client, k8s_apps_client, 'server', serverId)

def shutdownServer(k8s_client, k8s_apps_client, serverId):
    result = frontend.shutdownServer(serverId)
    print(result)

def put(key, value):
    result = clientList[random.choice(list(clientList.keys()))].put(key, value)
    print(result)

def get(key):
    result = clientList[random.choice(list(clientList.keys()))].get(key)
    print(result)

def printKVPairs(serverId):
    result = frontend.printKVPairs(serverId)
    print(result)

def loadDataset(thread_id, keys, load_vals, num_threads):
    start_idx = int((len(keys) / num_threads) * thread_id)
    end_idx = int(start_idx + (int((len(keys) / num_threads))))

    for idx in range(start_idx, end_idx):
        try:
            result = clientList[thread_id].put(keys[idx], load_vals[idx])
        except:
            print("[Error in thread %d] put request fail, key = %d, val = %d" % (thread_id, keys[idx], load_vals[idx]))
            return

def runWorkload(k8s_client, k8s_apps_client, prefix, thread_id,
                keys, load_vals, run_vals, num_threads, num_requests,
                put_ratio, test_consistency, crash_server, add_server, remove_server):
    request_count = 0
    start_idx = int((len(keys) / num_threads) * thread_id)
    end_idx = int(start_idx + (int((len(keys) / num_threads))))

    if test_consistency == 1:
        while num_requests > request_count:
            idx = random.randint(start_idx, end_idx - 1)
            if thread_id == 0 and request_count == int(num_requests / 2):
                if crash_server == 1:
                    killServer(k8s_client, k8s_apps_client, 0)
                elif add_server == 1:
                    addServer(k8s_client, k8s_apps_client, prefix)
                elif remove_server == 1:
                    shutdownServer(k8s_client, k8s_apps_client, 0)
            newval = random.randint(0, 1000000)
            try:
                clientList[thread_id].put(keys[idx], newval)
            except:
                print("[Error in thread %d] put request fail, key = %d, val = %d" % (thread_id, keys[idx], newval))
                return

            try:
                result = clientList[thread_id].get(keys[idx])
                result = result.split(':')
                if int(result[0]) != keys[idx] or int(result[1]) != newval:
                    print("[Error] request = (%d, %d), return = (%d, %d)" % (keys[idx], newval, int(result[0]), int(result[1])))
                    return
            except:
                print("[Error in thread %d] get request fail, key = %d", keys[idx])
                return
            request_count += 1
    else:
        optype = []
        for i in range(0, 100):
            if (i % 100) < put_ratio:
                optype.append("Put")
            else:
                optype.append("Get")
        random.shuffle(optype)

        while num_requests > request_count:
            for idx in range(start_idx, end_idx):
                if request_count == num_requests:
                    break
                if optype[idx % 100] == "Put":
                    try:
                        result = clientList[thread_id].put(keys[idx], run_vals[idx])
                    except:
                        print("[Error in thread %d] put request fail, key = %d, val = %d" % (thread_id, keys[idx], run_vals[idx]))
                        return
                elif optype[idx % 100] == "Get":
                    try:
                        result = clientList[thread_id].get(keys[idx])
                        result = result.split(':')
                        if int(result[0]) != keys[idx] or int(result[1]) != load_vals[idx]:
                            print("[Error] request = (%d, %d), return = (%d, %d)" % (keys[idx], load_vals[idx], int(result[0]), int(result[1])))
                            return
                    except:
                        print("[Error in thread %d] get request fail, key = %d", keys[idx])
                        return
                else:
                    print("[Error] unknown operation type")
                    return
                request_count += 1

def testKVS(k8s_client, k8s_apps_client, prefix, num_keys, num_threads,
            num_requests, put_ratio, test_consistency=0, crash_server=0,
            add_server=0, remove_server=0):
    serverList = frontend.listServer()
    serverList = serverList.split(',')
    if len(serverList) < 1:
        print("[Error] Servers do not exist")
        return

    if len(clientList) < num_threads:
        print("[Warning] Clients should exist more than # of threads")
        print("[Warning] Add %d more clients" % (num_threads - len(clientList)))
        for i in range(len(clientList), num_threads):
            addClient(k8s_client, k8s_apps_client, prefix)

    keys = list(range(0, num_keys))
    load_vals = list(range(0, num_keys))
    run_vals = list(range(num_keys, num_keys * 2))

    random.shuffle(keys);
    random.shuffle(load_vals);
    random.shuffle(run_vals);

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=num_threads)
    start = time.time()
    for thread_id in range(0, num_threads):
        pool.submit(loadDataset, thread_id, keys, load_vals, num_threads)
    pool.shutdown(wait=True)
    end = time.time()
    print("Load throughput = " + str(round(num_keys/(end - start), 1)) + "ops/sec")

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=num_threads)
    start = time.time()
    for thread_id in range(0, num_threads):
        pool.submit(runWorkload, k8s_client, k8s_apps_client, prefix,
                    thread_id, keys, load_vals, run_vals,
                    num_threads, int(num_requests / num_threads), put_ratio,
                    test_consistency, crash_server, add_server, remove_server)
    pool.shutdown(wait=True)
    end = time.time()
    print("Run throughput = " + str(round(num_requests/(end - start), 1)) + "ops/sec")

def init_cluster(k8s_client, k8s_apps_client, num_client, num_server, ssh_key, prefix):
    global frontend

    print('Creating a frontend pod...')
    frontend_spec = util.load_yaml('yaml/pods/frontend-pod.yml', prefix)
    env = frontend_spec['spec']['containers'][0]['env']
    k8s_client.create_namespaced_pod(namespace=util.NAMESPACE, body=frontend_spec)
    util.check_wait_pod_status(k8s_client, 'role=frontend', 'Running')
    frontend = xmlrpc.client.ServerProxy(baseAddr + str(baseFrontendPort))

    print('Creating server pods...')
    add_nodes(k8s_client, k8s_apps_client, 'server', num_server, prefix)

    print('Creating client pods...')
    add_nodes(k8s_client, k8s_apps_client, 'client', num_client, prefix)

def event_trigger(k8s_client, k8s_apps_client, prefix):
    terminate = False
    while terminate != True:
        cmd = input("Enter a command: ")
        args = cmd.split(':')

        if args[0] == 'addClient':
            addClient(k8s_client, k8s_apps_client, prefix)
        elif args[0] == 'addServer':
            addServer(k8s_client, k8s_apps_client, prefix)
        elif args[0] == 'listServer':
            listServer()
        elif args[0] == 'killServer':
            serverId = int(args[1])
            killServer(k8s_client, k8s_apps_client, serverId)
        elif args[0] == 'shutdownServer':
            serverId = int(args[1])
            shutdownServer(k8s_client, k8s_apps_client, serverId)
        elif args[0] == 'put':
            key = int(args[1])
            value = int(args[2])
            put(key, value)
        elif args[0] == 'get':
            key = int(args[1])
            get(key)
        elif args[0] == 'printKVPairs':
            serverId = int(args[1])
            printKVPairs(serverId)
        elif args[0] == 'testKVS':
            num_keys = int(args[1])
            num_threads = int(args[2])
            num_requests = int(args[3])
            put_ratio = int(args[4])
            test_consistency = int(args[5])
            crash_server = int(args[6])
            add_server = int(args[7])
            remove_server = int(args[8])
            testKVS(k8s_client, k8s_apps_client, prefix, num_keys, num_threads,
                    num_requests, put_ratio, test_consistency, crash_server,
                    add_server, remove_server)
        elif args[0] == 'terminate':
            terminate = True
        else:
            print("Unknown command")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='''Create a KVS cluster using Kubernetes
                                    and Kubespray. If no SSH key is specified, we use the
                                    default SSH key (~/.ssh/id_rsa), and we expect that
                                    the corresponding public key has the same path and ends
                                    in .pub. If no configuration file base is specified, we
                                    use the default ($KVS_HOME/conf/kvs-base.yml).''')

    if 'KVS_HOME' not in os.environ:
        os.environ['KVS_HOME'] = "/home/" + os.environ['USER'] + "/projects/cs380d-f23/project1/"

    parser.add_argument('-c', '--client', nargs=1, type=int, metavar='C',
                        help='The number of client nodes to start with ' +
                        '(required)', dest='client', required=True)
    parser.add_argument('-s', '--server', nargs=1, type=int, metavar='S',
                        help='The number of server nodes to start with ' +
                        '(required)', dest='server', required=True)
    parser.add_argument('--ssh-key', nargs='?', type=str,
                        help='The SSH key used to configure and connect to ' +
                        'each node (optional)', dest='sshkey',
                        default=os.path.join(os.environ['HOME'], '.ssh/id_rsa'))

    args = parser.parse_args()

    prefix = os.environ['KVS_HOME']

    k8s_client, k8s_apps_client = util.init_k8s()

    init_cluster(k8s_client, k8s_apps_client, args.client[0], args.server[0], args.sshkey, prefix)

    event_trigger(k8s_client, k8s_apps_client, prefix)