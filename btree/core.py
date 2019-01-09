import pdb
import uuid
from locking.lock import ReadWriteLock
from btree.insertion import Insertion
from btree.deletion import Deletion


class BTree:
    def __init__(self, max_keys = 2):
        # Create root node, and set up root identifier
        self.set_root(max_keys)
        self.num_keys = 0
        self.min_keys = max_keys // 2

    def get_root(self):
        return Node.get_node(self.root_id)

    def set_root(self, max_keys):
        initial_root = Node([], max_keys)
        initial_root.is_root = True
        self.root_id = initial_root.id

    def add_key(self, key):
        print("Adding key ", key)
        self.num_keys += 1

        root = self.get_root()
        res = root.add_key(key)
        
        if res is None:
            print("WTF")
        split_info = res['split_info']
        child_idx = res['child_idx']

        # If the root is a leaf and it overflowed handle here
        if split_info == 'leaf overflow':
            maybe_root = res['node']

            if self.root_id == maybe_root.id:
                print(self.root_id, maybe_root.id)
                root_split_info = maybe_root.split()

                if root_split_info:
                    keys = [root_split_info['median']]
                    max_keys = maybe_root.max_keys
                    max_key = Node.get_node(root_split_info['right_id']).max_key
                    children_ids = [root_split_info['left_id'], root_split_info['right_id']]
                    new_root = Node(keys, max_keys, max_key, children_ids)
                    new_root.is_root = True
                    maybe_root.is_root = False
                    self.root_id = new_root.id
                    maybe_root.release_write()
            else:
                print("The root done changed")
        # Otherwise if there is split info and the root is not a leaf
        elif split_info:

            maybe_root = res['node']
            maybe_root.acquire_write()

            # If this is still the root, then we must create a new root
            if self.root_id == maybe_root.id:
                print(self.root_id, maybe_root.id)
                root_split_info = maybe_root.handle_split(split_info, child_idx)

                if root_split_info:
                    keys = [root_split_info['median']]
                    max_keys = maybe_root.max_keys
                    max_key = Node.get_node(root_split_info['right_id']).max_key
                    children_ids = [root_split_info['left_id'], root_split_info['right_id']]
                    new_root = Node(keys, max_keys, max_key, children_ids)
                    new_root.is_root = True
                    maybe_root.is_root = False
                    self.root_id = new_root.id
            else:
                print("The root done changed")
                
            # if self.is_not_rightmost() and key > self.max_key:
            #     self.release_write()
            #     new_node = self.scan_right_for_write_guard(key)
            #     new_node.acquire_write()
            #     return new_node.handle_split(split_info, child_idx)


            return res

    # def add_key2(self, key):
    #     print("Adding key ", key)
    #     self.num_keys += 1

    #     root = self.get_root()
    #     current = root
    #     path = []

    #     while not current.is_leaf():
    #         path.append(current)
    #         next = current.scan_node(key)
    #         current = Node.get_node(next)

    #     current.add_key_leaf()

    def remove_key(self, key):
        self.num_keys -= 1
        root = self.get_root()
        root.remove_key(key)

        # Root may become empty through recursive rebalancing of tree
        # If so, this means it has one child and that will become the
        # new root
        if root.is_empty():
            new_root = Node.get_node(root.children_ids[0])
            self.root_id = new_root.id

    def print(self):
        curr_node = self.get_root()

        print("Printing tree...\n")
        print(curr_node.keys, curr_node.max_key)

        i = 0
        queue = []

        while not curr_node.is_leaf():
            children = curr_node.get_children()
            for _, child in enumerate(children):
                queue.append(child)

            curr_node = queue[i]
            i += 1

        for i, node in enumerate(queue):
            if node.is_not_rightmost():
                print(node.keys, node.max_key, node.link)
            else:
                print(node.keys, node.max_key, node.id)


        print("-----------------------")

class Node(Insertion, Deletion):
    # Class variable to store all nodes
    nodes = {}

    @classmethod
    def get_node(self, id):
        return self.nodes.get(id)

    def __init__(self, keys = [], max_keys = 2, max_key = None, children_ids = []):
        self.keys = keys
        self.max_keys = max_keys
        self.min_keys = max_keys // 2
        self.max_key = max_key
        self.id = uuid.uuid4()
        self.lock = ReadWriteLock()
        self.is_root = False

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

    def is_last_child(self, child_id):
        return child_id == self.children_ids[-1]

    def is_not_rightmost(self):
        return self.max_key is not None

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
    
    def scan_right_for_write_guard(self, key):
        print("We be scannin doe")
        curr_node = Node.get_node(self.link)

        while curr_node.max_key is not None and curr_node.max_key < key:
            curr_node = Node.get_node(self.link)

        return curr_node

    def scan_node(self, key):
        child_idx = self.find_idx(key)

        if self.is_not_rightmost() and key > self.max_key:
            new_node = self.scan_right_for_write_guard(key)
            return new_node.scan_node(key)

        return self.children_ids[child_idx]
        

### build tree
# btree = BTree(4)
# root = Node([13, 24, 30], 4)
# root.is_root = True
# child_one = Node([2, 3, 5, 7], 4, 14)
# child_two = Node([14, 16, 19, 22], 4, 24)
# child_three = Node([24, 27, 29], 4, 33)
# child_four = Node([33, 34, 38, 39], 4)
# root.children_ids = [child_one.id, child_two.id, child_three.id, child_four.id]
# btree.root_id = root.id
# child_one.link = child_two.id
# child_two.link = child_three.id
# child_three.link = child_four.id

# btree.print()
# ### Test adding keys
# # Add key to leaf
# btree.add_key(28)
# # Add key causing leaf split, while parent(root) has space
# btree.add_key(20)
# btree.print()

# # Add key causing leaf split, and parent(root) split -> increase depth of tree
# btree.add_key(8)

# # Add some more keys
# btree.add_key(10)
# btree.add_key(37)

# btree.print()

# ### Test removing keys
# # Remove key from leaf
# # btree.remove_key(39)
# # btree.remove_key(28)

# # # Remove key from leaf, causing rotation right (from left sibling)
# # btree.remove_key(33)

# # # Remove key from leaf, causing rotation left (from right sibling)
# # btree.remove_key(3)

# # # Remove key from leaf, causing merge left (from right sibling)
# # btree.remove_key(37)

# # # Setup for next test
# # btree.add_key(37)
# # btree.add_key(39)
# # btree.remove_key(24)

# # # Remove key from leaf, causing merge right (from left sibling)
# # btree.remove_key(27)

# # # Remove additional key to setup next test
# # btree.remove_key(16)

# # # Remove key causing rebalance propagating to the root, emptying it, and
# # # creating a new root
# # btree.remove_key(8)

# btree.print()
# # pdb.set_trace()
# print("done")
