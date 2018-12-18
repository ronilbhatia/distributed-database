import time
from threading import Thread, Lock
from random import *
from b_plus_tree import *
import pdb

btree = core.BPlusTree(4)

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
# t3 = Thread3()
# t4 = Thread4()
t.start()
t2.start()
# t3.start()
# t4.start()
t.join()
t2.join()
# t3.join()
# t4.join()
btree.print()
