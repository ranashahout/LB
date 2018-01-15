import datetime
import queue
import socket
import threading

TASK_PICTURE_TYPE = 'P'
TASK_VIDEO_TYPE = 'V'
TASK_MUSIC_TYPE = 'M'
SERVER_MUSIC_TYPE = 'M'
SERVER_VIDEO_TYPE = 'V'
SERVER_COUNT = 3
SERVER_TYPES = [SERVER_VIDEO_TYPE, SERVER_VIDEO_TYPE, SERVER_MUSIC_TYPE]
q_lock = threading.Lock()
now = datetime.datetime.now()

expected_finish = [now, now, now]
q_first = queue.Queue()
q_second = queue.Queue()
q_third = queue.Queue()


def q_add(buffer, client_socket):
    q_lock.acquire()
    # print("acquired queue lock")

    q_update(buffer, client_socket)
    q_lock.release()


def q_update(buffer, client_socket):
    task = buffer.decode("utf-8")
    # get all waiting tasks from queues.
    tasks = []
    for q in [q_first, q_second, q_third]:
        queue_get_all(q, tasks)

    tasks.append((buffer, client_socket, int(task[1])))

    # print("pending tasks: %s" % tasks)
    # schedule tasks to servers from scratch
    (srv1_tasks, srv2_tasks, srv3_tasks) = schedule_tasks(tasks)
    for task in srv1_tasks:
        q_first.put(task)
    for task in srv2_tasks:
        q_second.put(task)
    for task in srv3_tasks:
        q_third.put(task)

    # print("srv1_tasks: %s " % srv1_tasks)
    # print("srv2_tasks: %s " % srv2_tasks)
    # print("srv3_tasks: %s " % srv3_tasks)


def schedule_tasks(tasks):
    min_latest_time = datetime.datetime.now() + datetime.timedelta(weeks=1)
    best_perm = []
    perm_list = [([], [], [])]

    for t in tasks:
        perm_list = assign_jobs(perm_list, t)

    # print("made all permutations")

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

    # print('best permutation optimized: %s with finish time %s (%s seconds from now)' % (printable_perm, min_latest_time, seconds))
    return best_perm


def queue_get_all(q, tasks):
    while 1:
        try:
            tasks.append(q.get_nowait())
        except queue.Empty:
            break


def latest_time(p):
    # p is a tuple of 3 lists of tasks (which are tuples): ([tasks], [tasks], [tasks])

    times = []
    for index in range(SERVER_COUNT):
        end_time = expected_end_time(SERVER_TYPES[index], p[index], expected_finish[index])
        times.append(end_time)

    return max(times)


def calc_task_time(server_type: str, task_type: str, task_time: int) -> int:
    if server_type == SERVER_VIDEO_TYPE:
        if task_type == TASK_MUSIC_TYPE:
            return task_time * 2
    else:  # server is music type
        if task_type == TASK_VIDEO_TYPE:
            return task_time * 3
        elif task_type == TASK_PICTURE_TYPE:
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
        expected_finish[server_index] = datetime.timedelta(seconds=task_time) + datetime.datetime.now()
        q_lock.release()
        peer_name = str(client_socket.getpeername())
        # print("removed task %s from %s from queue %s" % (buffer.decode("utf-8"), peer_name, task_queue))

        srv_socket.sendall(buffer)
        # print("sent %s to %s" % (buffer.decode("utf-8"), srv_socket.getpeername()))

        buffer_from_server = srv_socket.recv(2)
        # print("received %s from %s" % (buffer_from_server.decode("utf-8"), srv_socket.getpeername()))

        expected_finish[server_index] = datetime.datetime.now()

        client_socket.sendall(buffer_from_server)
        # print("sent %s back to %s" % (buffer_from_server.decode("utf-8"), peer_name))

        client_socket.close()
        # print("closed TCP client socket %s" % peer_name)


server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# bind the socket to a public host, and a well-known port
server_socket.bind(("10.0.0.1", 80))
# become a server socket
server_socket.listen(5)

# create an INET, STREAMing socket
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
    # accept connections from outside
    (sockToClient, address) = server_socket.accept()
    # now do something with the clientsocket
    # in this case, we'll pretend this is a threaded server
    schedule_request(sockToClient)
