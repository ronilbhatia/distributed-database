from threading import Thread
from btree import *
import pdb

### Build Tree
btree = core.BTree(4)
root = core.Node([24], 4)
child_one_l1 = core.Node([5, 13], 4, 24)
child_two_l1 = core.Node([30, 36], 4)
child_one_l2 = core.Node([1, 2, 4], 4, 5)
child_two_l2 = core.Node([5, 7, 8], 4, 13)
child_three_l2 = core.Node([14, 16, 19, 22], 4, 24)
child_four_l2 = core.Node([25, 28, 29], 4, 30)
child_five_l2 = core.Node([32, 34, 35], 4, 36)
child_six_l2 = core.Node([37, 40], 4)

### Set children_ids appropriately
child_one_l1.acquire_write()
child_one_l1.set_children_ids([child_one_l2.get_id(), child_two_l2.get_id(), child_three_l2.get_id()])
child_one_l1.release_write()

child_two_l1.acquire_write()
child_two_l1.set_children_ids([child_four_l2.get_id(), child_five_l2.get_id(), child_six_l2.get_id()])
child_two_l1.release_write()

root.acquire_write()
root.set_children_ids([child_one_l1.get_id(), child_two_l1.get_id()])
root.release_write()
  
btree.root_id = root.get_id()
btree.print()

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
