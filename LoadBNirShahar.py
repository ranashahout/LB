 
# bhnir1
import socket,sys,time
import SocketServer
import threading

lock = threading.Lock()

def createSocket(addr, port):
    '''
socket.getaddrinfo(host, port[, family[, socktype[, proto[, flags]]]])
Translate the host/port argument into a sequence of 5-tuples that contain all the 
necessary arguments for creating a socket connected to that service.
host is a domain name, a string representation of an IPv4/v6 address or None.
port is a string service name such as 'http',
a numeric port number or None. By passing None as the value of host and port,
you can pass NULL to the underlying C API.
    '''
    for res in socket.getaddrinfo(addr, port, socket.AF_UNSPEC, socket.SOCK_STREAM):
        af, socktype, proto, canonname, sa = res
        try:
            new_sock = socket.socket(af, socktype, proto)
        except socket.error as msg:
            my_logger(msg)
            new_sock = None
            continue
        try:
            new_sock.connect(sa)
        except socket.error as msg:
            my_logger(msg)
            new_sock.close()
            new_sock = None
            continue
        break
    if new_sock is None:
        my_logger('could not open socket')
        sys.exit(1)
    return new_sock

def getNextServer(req_type,rec_time):
    global lock
    lock.acquire()
    if q1.get_task_total_time((req_type,rec_time)) <= q2.get_task_total_time((req_type,rec_time)):
        if q1.get_task_total_time((req_type,rec_time)) <= q3.get_task_total_time((req_type,rec_time)):
            next_server=1
        else:
            next_server=3
    elif q2.get_task_total_time((req_type,rec_time)) <= q3.get_task_total_time((req_type,rec_time)):
        next_server=2
    else :
        next_server=3
    lock.release()
    return next_server



class AbstractWorker(object):
    def __init__(self):
        self.queue = []
        self.total_time=0
    def add(self,task,time=0):
        self.queue.append((task,time))
        self.total_time+=time
    def pop(self):
        self.total_time-=(self.queue.pop())[1]
     

class VidoePicServer(AbstractWorker):
    def addTask(self,task):
        if task[0]=='M':
            self.add(task[0],task[1]*2)
        else:
            self.add(task[0],task[1])
    def get_task_total_time(self,task):
        if task[0]=='M':
            return self.total_time+task[1]*2
        else:
            return self.total_time+task[1]

class MusicServer(AbstractWorker):
    def addTask(self,task):
        if task[0]=='M':
            self.add(task[0],task[1])
        elif task[0]=='P':
            self.add(task[0],task[1]*2)
        else:
            self.add(task[0],task[1]*3)
    def get_task_total_time(self,task):
        if task[0]=='M':
            return self.total_time+task[1]
        elif task[0]=='P':
            return self.total_time+task[1]*2
        else:
            return self.total_time+task[1]*3

def my_logger(string):
    print '%s: %s' % (time.strftime('%H:%M:%S', time.localtime(time.time())), string)


def my_parser(request):
    return (request[0], int(request[1]))


class LBHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        client_sock = self.request
        req = client_sock.recv(2)
        req_type, req_time = my_parser(req)
        server_id = getNextServer(req_type,int(req_time))
        q=getServerQueue(server_id)
        q.addTask((req_type, req_time))
        my_logger('recieved request %s from %s, sending to %s' % (req, self.client_address[0], getServerAddr(server_id)))
        serv_sock = getServerSocket(server_id)
        serv_sock.sendall(req)
        data = serv_sock.recv(2)
        q.pop()
        client_sock.sendall(data)
        client_sock.close()


class multithred_server(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass


servers = {'server1': ('192.168.0.101', None),'server2': ('192.168.0.102', None),'server3': ('192.168.0.103', None)}
q1=VidoePicServer()
q2=VidoePicServer()
q3=MusicServer()
my_queues=[q1,q2,q3]

def getServerAddr(ID):
    name = 'server%d' % ID
    return servers[name][0]

def getServerSocket(ID):
    name = 'server%d' % ID
    return servers[name][1]

def getServerQueue(ID):
    return my_queues[ID-1]


if __name__ == '__main__':
    try:
        my_logger('LB Started')
        my_logger('Connecting to servers')
        for name, (ip_addr, sock) in servers.iteritems():
            servers[name] = (ip_addr, createSocket(ip_addr, 80))

        server = multithred_server(('10.0.0.1', 80), LBHandler)
        server.serve_forever()
    except Exception as e:
        my_logger(e)