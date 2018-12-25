import pdb
import uuid
from locking.lock import ReadWriteLock
from locking.lock_path import LockPath
from btree.insertion import Insertion
from btree.deletion import Deletion


class BTree:
    def __init__(self, max_keys = 2):
        self.root = Node([], max_keys)
        self.num_keys = 0
        self.min_keys = max_keys // 2

    def add_key(self, key):
        print("Adding key ", key)
        self.num_keys += 1

        print("Acquiring read lock on root with keys ", self.root.keys)
        self.root.acquire_read()
        lock_path = LockPath()
        lock_path.add_read(self.root)

        res = self.root.add_key(key, lock_path)

        if res:
            self.root.keys = [res['median']]
            self.root.children_ids = [res['left_id'], res['right_id']]

            print("Releasing write lock on new root with keys ", self.root.keys)
            self.root.release_write()
            return res

    def remove_key(self, key):
        self.num_keys -= 1
        self.root.remove_key(key)

        # Root may become empty through recursive rebalancing of tree
        # If so, this means it has one child and that will become the
        # new root
        if self.root.is_empty():
            self.root = Node.get_node(self.root.children_ids[0])

    def print(self):
        curr_node = self.root

        print("Printing tree...\n")
        print(curr_node.keys)

        i = 0
        queue = []

        while not curr_node.is_leaf():
            children = curr_node.get_children()
            for _, child in enumerate(children):
                queue.append(child)

            curr_node = queue[i]
            i += 1

        for i, node in enumerate(queue):
            print(node.keys)

        print("-----------------------")

class Node(Insertion, Deletion):
    # Class variable to store all nodes
    nodes = {}

    @classmethod
    def get_node(self, id):
        return self.nodes.get(id)

    def __init__(self, keys = [], max_keys = 2, children_ids = []):
        self.keys = keys
        self.max_keys = max_keys
        self.min_keys = max_keys // 2
        self.id = uuid.uuid4()
        self.lock = ReadWriteLock()

        # Add node to hash map, ensuring it has a unique id
        while Node.get_node(self.id) is not None:
            self.id = uuid.uuid4()
        Node.nodes[self.id] = self

        self.children_ids = children_ids

    def get_child_at_index(self, child_idx):
        if child_idx < 0 or child_idx >= len(self.children_ids):
            return None
        else:
            return Node.get_node(self.children_ids[child_idx])

    # gives a list comprehension, taking each child id and looking up in
    # hash map
    def get_children(self):
        # children = map(lambda id: Node.get_node(id), self.children_ids)
        children = [Node.get_node(id) for id in self.children_ids]
        return children

    def num_keys(self):
        return len(self.keys)

    def is_full(self):
        return self.num_keys() == self.max_keys

    def is_stable(self):
        return self.num_keys() < self.max_keys

    def overflow(self):
        return self.num_keys() > self.max_keys

    def is_empty(self):
        return self.num_keys() == 0

    def is_leaf(self):
        return len(self.children_ids) == 0

    def can_give_up_keys(self):
        return self.min_keys < self.num_keys()

    def is_deficient(self):
        return self.num_keys() < self.min_keys

    def acquire_read(self):
        self.lock.acquire_read()

    def acquire_write(self):
        self.lock.acquire_write()

    def release_read(self):
        self.lock.release_read()

    def release_write(self):
        self.lock.release_write()

    def find_idx(self, key):
        found_idx = False
        idx = None

        for i, curr_key in enumerate(self.keys):
            if key < curr_key:
                found_idx = True
                idx = i
                break

        if not found_idx:
            idx = self.num_keys()

        return idx

### build tree
btree = BTree(4)
btree.root = Node([13, 24, 30], 4)
child_one = Node([2, 3, 5, 7], 4)
child_two = Node([14, 16, 19, 22], 4)
child_three = Node([24, 27, 29], 4)
child_four = Node([33, 34, 38, 39], 4)
btree.root.children_ids = [child_one.id, child_two.id, child_three.id, child_four.id]

btree.print()
### Test adding keys
# Add key to leaf
btree.add_key(28)
# Add key causing leaf split, while parent(root) has space
btree.add_key(20)
btree.print()

# Add key causing leaf split, and parent(root) split -> increase depth of tree
btree.add_key(8)

# Add some more keys
btree.add_key(10)
btree.add_key(37)

# btree.print()

### Test removing keys
# Remove key from leaf
btree.remove_key(39)
btree.remove_key(28)

# Remove key from leaf, causing rotation right (from left sibling)
btree.remove_key(33)

# Remove key from leaf, causing rotation left (from right sibling)
btree.remove_key(3)

# Remove key from leaf, causing merge left (from right sibling)
btree.remove_key(37)

# Setup for next test
btree.add_key(37)
btree.add_key(39)
btree.remove_key(24)

# Remove key from leaf, causing merge right (from left sibling)
btree.remove_key(27)

# Remove additional key to setup next test
btree.remove_key(16)

# Remove key causing rebalance propagating to the root, emptying it, and
# creating a new root
btree.remove_key(8)

btree.print()
# pdb.set_trace()
print("done")
