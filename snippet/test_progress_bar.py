import sys, math
import time




def wait_fun():
    time.sleep(1)

def bar():
    total_size = 10
    downloaded = 0

    start = time.clock()
    while True:
        downloaded +=1

        done = int(50 * downloaded / total_size)

        sys.stdout.write('\r[{1}{2}]{0:3.0f}% {3}ps'
            .format(math.floor((float(downloaded)
                / total_size) * 100),
            '=' * done,
            ' ' * (50 - done),
            (downloaded // (time.clock() - start)) / 8))

        # sys.stdout.flush()

        if downloaded == 10:
            break

        wait_fun()


def _test_print():
    c = 0
    while True:
        c+=1
        if c>10:
            break

        sys.stdout.write('\r%s'%('='*c,))
        sys.stdout.flush()
        time.sleep(1)


def _test_print2():
    c = 0
    while True:
        c+=1
        if c>10:
            break

        sys.stdout.write('\r%s'%('='*c,))
        sys.stdout.flush()
        print 'hithere'

        time.sleep(1)



if __name__ =='__main__':
    # bar()
    _test_print2()