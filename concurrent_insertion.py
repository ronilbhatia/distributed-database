import time
from threading import Thread
from random import *
from btree import *
import pdb

btree = core.BTree(4)
# btree.root = core.Node([13, 24, 30], 4)
# child_one = core.Node([2, 3, 5, 7], 4)
# child_two = core.Node([14, 16, 19, 22], 4)
# child_three = core.Node([24, 27, 29], 4)
# child_four = core.Node([33, 34, 38, 39], 4)
# btree.root.children_ids = [child_one.id, child_two.id, child_three.id, child_four.id]

class Thread1(Thread):
    def run(self):
        global btree
        for i in range(10):
            btree.add_key(i)

class Thread2(Thread):
    def run(self):
        global btree
        for i in range(10):
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
print(btree.root_id)

# for i in range(10):
#     btree.add_key(i)

# btree.print()