from threading import Thread
from btree import *
import pdb

btree = core.BTree(4)
NUM_INSERTS = 1000

class Thread1(Thread):
    def run(self):
        global btree
        for i in range(NUM_INSERTS):
            btree.add_key(i)

class Thread2(Thread):
    def run(self):
        global btree
        for i in range(NUM_INSERTS):
            btree.add_key(i+NUM_INSERTS)

class Thread3(Thread):
    def run(self):
        global btree
        for i in range(NUM_INSERTS):
            btree.add_key(i+NUM_INSERTS * 2)

class Thread4(Thread):
    def run(self):
        global btree
        for i in range(NUM_INSERTS):
            btree.add_key(i+NUM_INSERTS * 3)

t1 = Thread1()
t2 = Thread2()
t3 = Thread3()
t4 = Thread4()

t1.start()
t2.start()
t3.start()
t4.start()

t1.join()
t2.join()
t3.join()
t4.join()

btree.print()
print(btree.root_id)