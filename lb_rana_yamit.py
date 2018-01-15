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


def q_add(buffer, client_socket):
    q_lock.acquire()
    task = buffer.decode("utf-8")

    tasks = []
    for q in [q_first, q_second, q_third]:
        queue_get_all(q, tasks)

    tasks.append((buffer, client_socket, int(task[1])))

    (tasks_of_1, tasks_of_2, tasks_of_3) = sched(tasks)
    for task in tasks_of_1:
        q_first.put(task)
    for task in tasks_of_2:
        q_second.put(task)
    for task in tasks_of_3:
        q_third.put(task)
    q_lock.release()


def sched(tasks):
    min_latest_time = datetime.datetime.now() + datetime.timedelta(weeks=1)
    best_perm = []
    perm_list = [([], [], [])]

    for t in tasks:
        perm_list = assign_jobs(perm_list, t)

    perms_with_times = [(latest_time(p), p) for p in perm_list]

    for (time, p) in perms_with_times:
        if time < min_latest_time:
            min_latest_time = time
            best_perm = p

    optimize_permutation(best_perm)

    seconds = (min_latest_time - datetime.datetime.now()).seconds
    printable_perm = ([task[0] for task in best_perm[0]],
                      [task[0] for task in best_perm[1]],
                      [task[0] for task in best_perm[2]])

    return best_perm


def queue_get_all(q, tasks):
    while 1:
        try:
            tasks.append(q.get_nowait())
        except queue.Empty:
            break


def latest_time(p):

    times = []
    for index in range(SERVERS_NUM):
        end_time = expected_end_time(SERVER_TYPES[index], p[index], exp[index])
        times.append(end_time)

    return max(times)


def calc_task_time(server_type: str, task_type: str, task_time: int) -> int:
    if server_type == VIDEO_S:
        if task_type == MUSIC:
            return task_time * 2
    else: 
        if task_type == VIDEO:
            return task_time * 3
        elif task_type == PIC:
            return task_time * 2
    return task_time


def expected_end_time(server_type: str, task_list: [(bytes, socket.socket, int)],
                      start_time: datetime.datetime) -> datetime.datetime:
    work_time = 0

    for task in task_list:
        t_type = chr(task[0][0])
        t_time = task[2]
        work_time += calc_task_time(server_type, t_type, t_time)

    if start_time < datetime.datetime.now():
        start_time = start_time.now()

    return start_time + datetime.timedelta(seconds=work_time)


def optimize_permutation(perm):
    for index in range(len(perm)):
        task_list = perm[index]
        if len(task_list) > 1:
            task_list.sort(key=lambda t: calc_task_time(SERVER_TYPES[index], chr(t[0][0]), t[2]))


def assign_jobs(tuple_list, job):
    ret_val = []

    for t in tuple_list:
        new_tup = (t[0][:], t[1][:], t[2][:])
        new_tup[0].append(job)
        ret_val.append(new_tup)

        new_tup = (t[0][:], t[1][:], t[2][:])
        new_tup[1].append(job)
        ret_val.append(new_tup)

        new_tup = (t[0][:], t[1][:], t[2][:])
        new_tup[2].append(job)
        ret_val.append(new_tup)

    return ret_val


def schedule_request(client_socket):
    buffer = client_socket.recv(2)
    print("received ", buffer.decode("utf-8"), " from ", client_socket.getpeername())
    q_add(buffer, client_socket)


def handle_task(task_queue, srv_socket, server_index):
    while True:
        q_lock.acquire()
        try:
            (buffer, client_socket, task_time) = task_queue.get_nowait()
        except queue.Empty:
            q_lock.release()
            continue
        exp[server_index] = datetime.timedelta(seconds=task_time) + datetime.datetime.now()
        q_lock.release()
        peer_name = str(client_socket.getpeername())

        srv_socket.sendall(buffer)

        buffer_from_server = srv_socket.recv(2)

        exp[server_index] = datetime.datetime.now()

        client_socket.sendall(buffer_from_server)

        client_socket.close()

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

thread_first = threading.Thread(args=(q_first, socket_first, 0), target=handle_task)
thread_second = threading.Thread(args=(q_second, socket_second, 1), target=handle_task)
thread_third = threading.Thread(args=(q_third, socket_third, 2), target=handle_task)

thread_first.start()
thread_second.start()
thread_third.start()

while True:

    (sockToClient, address) = server_socket.accept()

    schedule_request(sockToClient)
