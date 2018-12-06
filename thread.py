import time
from threading import Thread, Lock
from random import *

class ReadWriteLock(object):
    """ An implementation of a read-write lock for Python.
    Any number of readers can work simultaneously but they
    are mutually exclusive with any writers (which can
    only have one at a time).

    This implementation is reader biased. This can be harmful
    under heavy load because it can starve writers.
    However under light load it will be quite perfomant since
    reads are usually much less resource intensive than writes,
    and because it maximizes concurrency.
    """


    def __init__(self):
        self.__monitor = Lock()
        self.__exclude = Lock()
        self.readers = 0


    def acquire_read(self):
        with self.__monitor:
            self.readers += 1
            if self.readers == 1:
                self.__exclude.acquire()

    def release_read(self):
        with self.__monitor:
            self.readers -= 1
            if self.readers == 0:
                self.__exclude.release()


    def acquire_write(self):
        self.__exclude.acquire()

    def release_write(self):
        self.__exclude.release()

i = 0
j = 0
i_lock = ReadWriteLock()
j_lock = ReadWriteLock()
class PrintThread(Thread):
    def run(self):
        print("Hello World")
        global i, j
        for _ in range(10000):
            time.sleep(.00001)
            rand = randint(1, 100)
            if rand < 50:
                i_lock.acquire_read()
                j_lock.acquire_read()
                if (i != j):
                    print(i, j)
                i_lock.release_read()
                j_lock.release_read()
            elif rand < 75:
                i_lock.acquire_write()
                i += 1
                j_lock.acquire_write()
                j += 1
                time.sleep(.00001)
                i_lock.release_write()
                j_lock.release_write()
            else:
                j_lock.acquire_write()
                j += 1
                i_lock.acquire_write()
                i += 1
                time.sleep(.00001)
                j_lock.release_write()
                i_lock.release_write()

t = PrintThread()
t2 = PrintThread()
t.start()
t2.start()
t.join()
t2.join()
print(i, j)
