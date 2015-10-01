import math
from multiprocessing import Pool

REPITITION = 10
STEP = 5000
START = 10000
STOP = 600000

PROCESSES = 10

class time_tracker(object):
    """track number of function calls, cost of time, and total time"""
    def __init__(self, f):
        """function pass to the constructor"""
        self.f = f
        self.counter = 0
        self.total_time = 0
        self.__name__ = f.__name__
    def __call__(self, *args):
        from time import time, ctime
        start_time = time()

        # call function
        for i in range(REPITITION):
            result = self.f(*args)

        end_time = time()
        # in ms
        spent_time = (end_time - start_time) * 1000
        self.counter += 1
        self.total_time += spent_time

        # time tracker output
        # if self.counter % 50 == 0:
        print(ctime(),'[TIME]Function', '\''+ self.__name__ + '\'', 'called', REPITITION, 'times')
        print(ctime(),'[TIME]Time spent:,', '{:.2f}'.format(spent_time), 'ms')
        print(ctime(),'[TIME]Total time spent:', '{:.2f}'.format(self.total_time), 'ms')

        return result



global primes
primes = set()

def log(result):
    global primes

    if result:
        # since the result is a batch of primes, we have to use 
        # update instead of add (or for a list, extend instead of append)
        primes.update(result)

def isPrime(n):
    if n < 2:
        return False
    if n == 2:
        return True, n

    max = int(math.ceil(math.sqrt(n)))
    i = 2
    while i <= max:
        if n % i == 0:
            return False
        i += 1
    return True, n

def isPrimeWorker(start, stop):
    """
    find a batch of primes
    """
    primes = set()
    for i in range(start, stop):
        if isPrime(i):
            primes.add(i)

    return primes

@time_tracker
def single_version():
    
    global primes
    primes = set()
    for i in range(START, STOP):
        if isPrime(i):
            primes.add(i)

    # print(sum(primes))


@time_tracker
def multi_version():

    
    # chunks, but another value might be optimal


    global primes

    pool = Pool(processes=PROCESSES)

    # pick an arbitrary chunk size, this will give us 100 different 


    # use range instead of range, we don't actually need a list, just
    # the values in that range.
    for i in range(START, STOP, STEP):
        # call the *worker* function with start and stop values.
        pool.apply_async(isPrimeWorker,(i, i+STEP,), callback = log)

    pool.close()
    pool.join()

    # print(sum(primes))

if __name__ == "__main__":
    print('One process only:')
    single_version()
    print('Process: ', PROCESSES)
    multi_version()