import time
from threading import Thread, Lock
from random import *
from b_plus_tree import BPlusTree
import pdb

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

btree = BPlusTree(4)

class Thread1(Thread):
    def run(self):
        global btree
        for i in range(1000):
            btree.add_key(i)

class Thread2(Thread):
    def run(self):
        global btree
        for i in range(1000):
            btree.add_key(i+1000)

class Thread3(Thread):
    def run(self):
        global btree
        for i in range(1000):
            btree.add_key(i+2000)

class Thread4(Thread):
    def run(self):
        global btree
        for i in range(1000):
            btree.add_key(i+3000)


t = Thread1()
t2 = Thread2()
t3 = Thread3()
t4 = Thread4()
t.start()
t2.start()
t3.start()
t4.start()
t.join()
t2.join()
t3.join()
t4.join()
btree.print()
