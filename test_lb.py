import copy
import datetime
import queue
import threading
import socket

__author__ = 'avner'

TASK_PICTURE_TYPE = 'P'
TASK_VIDEO_TYPE = 'V'
TASK_MUSIC_TYPE = 'M'
SERVER_MUSIC_TYPE = 'M'
SERVER_VIDEO_TYPE = 'V'
SERVER_COUNT = 3
SERVER_TYPES = [SERVER_VIDEO_TYPE, SERVER_VIDEO_TYPE, SERVER_MUSIC_TYPE]
queue_lock = threading.Lock()
now = datetime.datetime.now()

expected_finish = [now, now, now]
srv1Queue = queue.Queue()
srv2Queue = queue.Queue()
srv3Queue = queue.Queue()


def add_to_queue(buffer, client_socket):
    queue_lock.acquire()

    update_queues(buffer, client_socket)
    queue_lock.release()


def update_queues(buffer, client_socket):
    task = buffer.decode("utf-8")
    # get all waiting tasks from queues.
    tasks = []
    for q in [srv1Queue, srv2Queue, srv3Queue]:
        queue_get_all(q, tasks)

    tasks.append((buffer, client_socket, int(task[1])))
    # schedule tasks to servers from scratch
    (srv1_tasks, srv2_tasks, srv3_tasks) = schedule_tasks(tasks)
    for task in srv1_tasks:
        srv1Queue.put(task)
    for task in srv2_tasks:
        srv2Queue.put(task)
    for task in srv3_tasks:
        srv3Queue.put(task)

    print("srv1_tasks: %s " % srv1_tasks)
    print("srv2_tasks: %s " % srv2_tasks)
    print("srv3_tasks: %s " % srv3_tasks)


def schedule_tasks(tasks):
    min_latest_time = datetime.datetime.now() + datetime.timedelta(weeks=1)
    min_perm = []
    perm_list = [([], [], [])]

    for t in tasks:
        perm_list = assign_jobs(perm_list, t)

    perms_with_times = [(latest_time(p), p) for p in perm_list]

    for (time, p) in perms_with_times:
        if time < min_latest_time:
            min_latest_time = time
            min_perm = p

    optimize_permutation(min_perm)

    seconds = (min_latest_time - datetime.datetime.now()).seconds
    printable_perm = ([task[0] for task in min_perm[0]],
                      [task[0] for task in min_perm[1]],
                      [task[0] for task in min_perm[2]])

    print('best permutation: %s with finish time %s (%s seconds from now)' % (printable_perm, min_latest_time, seconds))
    return min_perm


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


if __name__ == '__main__':
    str_test_tasks = ["M2", "V3", "M3", "P3", "V5", "P6", "V9"]
    dummy_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    test_tasks = [(str_task.encode('utf-8'), dummy_socket, int(str_task[1:])) for str_task in str_test_tasks]
    # print('converted tasks: %s' % test_tasks)

    # selected_perm = schedule_tasks(test_tasks)
    # optimize_permutation(selected_perm)
    #
    # # print('expected_finish: %s' % expected_finish)
    # prev_expected_times = copy.deepcopy(expected_finish)
    #
    # delta = []
    # for i in range(3):
    #     expected_finish[i] = expected_end_time(SERVER_TYPES[i], selected_perm[i], expected_finish[i])
    #     delta.append((expected_finish[i] - prev_expected_times[i]).seconds)
    #     jobs = [task[0] for task in selected_perm[i]]
    #     print('server %s expected finish: %s tasks: %s' % (i, expected_finish[i] - prev_expected_times[i], jobs))
    #
    # print('deltas: %s' % delta)

    for str_task in str_test_tasks:
        update_queues(str_task.encode('utf-8'), dummy_socket)
