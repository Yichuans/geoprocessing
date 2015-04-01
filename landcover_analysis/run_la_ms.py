import multiprocessing
import time
import os, sys
from landcover_analysis import process_each_wh_site, write_each_wh_result, get_all_wh_id

# the number of cores used - the following ensures there is one core remaining for other tasks
WORKER = multiprocessing.cpu_count() - 2

# specify num of workers
WORKER = 4

# -------------- NEED MODIFICATION ---------------
def job(job_id):
    # ADD: the main work process, 
    # the result should catch errors
    result = process_each_wh_site(job_id)

    return result

def get_queue():
    # create a queue to be populated by a list of ids to process
    q = multiprocessing.Queue()
    
    ids = get_all_wh_id()
    print 'Jobs to process:', len(ids)

    # ADD: queue logic here
    for wh_id in ids:
        q.put(wh_id)

    return q

def process_result(result):
    # ADD: process result logic here
    write_each_wh_result(result)

# --------------- TEMPLATE -----------------------
def worker_writer(q_out):
    while True:
        # get result from q_out
        result = q_out.get()
        if result == 'STOP':
            break

        process_result(result)


def worker(q, q_out):
    while True:
        # monitoring
        if q.qsize() %100 == 0:
            print 'Remaining jobs:', q.qsize()

        # get and ID from job id queue
        job_id = q.get()
        if job_id == 'STOP':
            break

        result = job(job_id)
        q_out.put(result)


def main():
    print 'Total number of workers:', WORKER
    # get queue
    q_out = multiprocessing.Queue()

    # Add queue of a list of ids to process
    q = get_queue()

    # setup and run worker processes
    p_workers = list()
    for i in range(WORKER):
        print 'Starting worker process:', i
        p = multiprocessing.Process(target=worker, args=(q, q_out))
        p_workers.append(p)
        
    # start
    for p in p_workers:
        p.start()

    # add stop flag to the queue
    for p in p_workers:
        q.put('STOP')

    # setup and run writer process
    p_w = multiprocessing.Process(target=worker_writer, args=(q_out,))
    p_w.start()


    # wait for workers to terminate
    for p in p_workers:
        p.join()

    # add stop signal for processing result
    q_out.put('STOP')

    # wait for the writer to finish
    p_w.join()


if __name__ == '__main__':
    main()