import datetime
import queue
import socket
import threading


MUSIC_S = 'M'
VIDEO_S = 'V'
SERVERS_NUM = 3
VIDEO = 'V'
PIC = 'P'
MUSIC = 'M'
SERVER_TYPES = [VIDEO_S, VIDEO_S, MUSIC_S]
q_lock = threading.Lock()
current = datetime.datetime.now()

exp = [current, current, current]
q_first = queue.Queue()
q_second = queue.Queue()
q_third = queue.Queue()

def sched(tasks):
    begin_time = datetime.datetime.now() + datetime.timedelta(weeks=1)
    best_perm = []
    arr_permutation = [([], [], [])]

    for t in tasks:
        arr_permutation = apply_tasks(arr_permutation, t)

    permutations = [(last_t(p), p) for p in arr_permutation]

    for (t, p) in permutations:
        if t < begin_time:
            begin_time = t
            best_perm = p

    opt_perm(best_perm)

    sec = (begin_time - datetime.datetime.now()).seconds
    perm_to_print = ([task[0] for task in best_perm[0]],
                      [task[0] for task in best_perm[1]],
                      [task[0] for task in best_perm[2]])
    print('opt permutation: %s, finish time %s (%s seconds)' % (perm_to_print, begin_time, sec))

    return best_perm

def apply_tasks(list, task):
    res = []

    for t in list:
        curr = (t[0][:], t[1][:], t[2][:])
        curr[0].append(task)
        res.append(curr)

        curr = (t[0][:], t[1][:], t[2][:])
        curr[1].append(task)
        res.append(curr)

        curr = (t[0][:], t[1][:], t[2][:])
        curr[2].append(task)
        res.append(curr)

    return res


######## Queue functions ######
def q_add(buff, client_socket):
    q_lock.acquire()
    task = buff.decode("utf-8")

    tasks = []
    for q in [q_first, q_second, q_third]:
        q_get(q, tasks)

    tasks.append((buff, client_socket, int(task[1])))

    (tasks_of_1, tasks_of_2, tasks_of_3) = sched(tasks)
    for task in tasks_of_1:
        q_first.put(task)
    for task in tasks_of_2:
        q_second.put(task)
    for task in tasks_of_3:
        q_third.put(task)
    q_lock.release()


def q_get(q, tasks):
    while 1:
        try:
            tasks.append(q.get_nowait())
        except queue.Empty:
            break

############## Time functions ################
def last_t(p):

    t = []
    for i in range(SERVERS_NUM):
        final = when_will_end(SERVER_TYPES[i], p[i], exp[i])
        t.append(final)

    return max(t)


def task_find_time(server_type: str, task_type: str, task_time: int) -> int:
    if server_type == VIDEO_S:
        if task_type == MUSIC:
            return task_time * 2
    else: 
        if task_type == VIDEO:
            return task_time * 3
        elif task_type == PIC:
            return task_time * 2
    return task_time


def when_will_end(server_type: str, tasks: [(bytes, socket.socket, int)],
                      begin: datetime.datetime) -> datetime.datetime:
    time_job = 0

    for task in tasks:
        t_type = chr(task[0][0])
        t_time = task[2]
        time_job += task_find_time(server_type, t_type, t_time)

    if begin < datetime.datetime.now():
        begin = begin.now()

    return begin + datetime.timedelta(seconds=time_job)

######################## Scheduling #################
def opt_perm(perm):
    for i in range(len(perm)):
        tasks = perm[i]
        if len(tasks) > 1:
            tasks.sort(key=lambda t: task_find_time(SERVER_TYPES[i], chr(t[0][0]), t[2]))


def ask_for_sched(client_socket):
    buff = client_socket.recv(2)
    print("receive ", buff.decode("utf-8"), " from ", client_socket.getpeername())
    q_add(buff, client_socket)


def thread_h(task_queue, svr_sock, server_index):
    while True:
        q_lock.acquire()
        try:
            (buff, client_socket, task_time) = task_queue.get_nowait()
        except queue.Empty:
            q_lock.release()
            continue
        exp[server_index] = datetime.timedelta(seconds=task_time) + datetime.datetime.now()
        q_lock.release()
        peer_name = str(client_socket.getpeername())

        svr_sock.sendall(buff)

        server_buff = svr_sock.recv(2)

        exp[server_index] = datetime.datetime.now()

        client_socket.sendall(server_buff)

        client_socket.close()
##################################################


server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

server_socket.bind(("10.0.0.1", 80))
server_socket.listen(5)

socket_first = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
socket_first.connect(("192.168.0.101", 80))
socket_second = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
socket_second.connect(("192.168.0.102", 80))
socket_third = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
socket_third.connect(("192.168.0.103", 80))

thread_first = threading.Thread(args=(q_first, socket_first, 0), target=thread_h)
thread_second = threading.Thread(args=(q_second, socket_second, 1), target=thread_h)
thread_third = threading.Thread(args=(q_third, socket_third, 2), target=thread_h)

thread_first.start()
thread_second.start()
thread_third.start()

while True:

    (sockToClient, address) = server_socket.accept()

    ask_for_sched(sockToClient)
