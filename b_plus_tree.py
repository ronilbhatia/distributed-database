import pdb
import uuid
from lock import ReadWriteLock

class BPlusTree:
    def __init__(self, max_keys = 2):
        self.root = Node([], max_keys)
        self.num_keys = 0
        self.min_keys = max_keys // 2

    def add_key(self, key):
        print("Adding key ", key)
        self.num_keys += 1

        print("Acquiring lock on root with keys ", self.root.keys)
        self.root.lock.acquire_write()
        lock_path = [self.root]

        res = self.root.add_key(key, lock_path)

        if res:
            self.root.keys = [res['median']]
            self.root.children_ids = [res['left_id'], res['right_id']]
            
            print("Releasing lock on new root with keys ", self.root.keys)
            self.root.lock.release_write()
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

class Node:
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

    def add_key(self, key, lock_path = []):
        if self.is_leaf():
            return self.add_key_leaf(key, lock_path)
        else:
            return self.add_key_internal(key, lock_path)


    def add_key_leaf(self, key, lock_path):
        key_idx = self.find_idx(key)
        self.keys.insert(key_idx, key)

        # if the node is overflowed we need to split it
        if self.overflow():
            return self.split()
        else:
            print("Releasing lock for leaf with keys ", self.keys)
            self.lock.release_write()

    def add_key_internal(self, key, lock_path):
        children = self.get_children()

        # if the node is not a leaf we must find the appropriate
        # child to attempt to add the key to
        child_idx = self.find_idx(key)
        child = children[child_idx]
        child.lock.acquire_write()

        if child.is_stable():
            for idx, node in enumerate(lock_path):
                node.lock.release_write()

            lock_path = []

        lock_path.append(child)
        split_info = children[child_idx].add_key(key, lock_path)

        # if we have split_info that implies the child had to be split
        if split_info:
            return self.handle_split(split_info, child_idx)


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

    def split(self):
        mid_idx = self.num_keys()//2

        # split up the node into left, right, and median
        median = self.keys[mid_idx]
        max_keys = self.max_keys

        left_keys = self.keys[0:mid_idx]
        left_children_ids = self.children_ids[0:mid_idx+1]

        # If node is leaf, we want to keep the median value in the right
        # node as well, but if it is not then we don't
        if self.is_leaf():
            right_keys = self.keys[mid_idx:]
        else:
            right_keys = self.keys[mid_idx+1:]
        right_children_ids = self.children_ids[mid_idx+1:]

        left = Node(left_keys, max_keys, left_children_ids)
        right = Node(right_keys, max_keys, right_children_ids)

        return {'median': median, 'left_id': left.id, 'right_id': right.id, 'orphan': self}

    def handle_split(self, split_info, child_idx):
        median = split_info['median']
        left_id = split_info['left_id']
        right_id = split_info['right_id']
        orphan = split_info['orphan']

        # We will add the left and right nodes as children of
        # the node on either side of the median value regardless
        # of whether the node is full or not and must be split.
        self.children_ids[child_idx] = left_id
        self.children_ids.insert(child_idx + 1, right_id)
        self.keys.insert(child_idx, median)

        # Release lock on orphaned node
        orphan.lock.release_write()

        # If the node is overflowed we must split it.
        if self.overflow():
            return self.split()
        else:
            print("Releasing lock on node with keys ", self.keys)
            self.lock.release_write()

    def remove_key(self, key):
        # Check if node is leaf - attempt to remove key directly
        if self.is_leaf():
            return self.remove_key_leaf(key)
        else:
            return self.remove_key_internal(key)

    def remove_key_leaf(self, key):
        try:
            self.keys.remove(key)
        except:
            print("key does not exist")
            return

        # Check it underflow has occurred and need to rebalance
        if self.is_deficient():
            return True

    def remove_key_internal(self, key):
        children = self.get_children()
        child_idx = self.find_idx(key)

        res = children[child_idx].remove_key(key)

        # having a res implies underflow occurred when removing key
        # from child and we must restructure the tree
        if res:
            return self.rebalance(child_idx)

    def rebalance(self, child_idx):
        child = self.get_child_at_index(child_idx)
        left_sibling = self.get_child_at_index(child_idx - 1)
        right_sibling = self.get_child_at_index(child_idx + 1)

        # Try to rotate from right sibling first, if it can give up keys
        if right_sibling is not None and right_sibling.can_give_up_keys():
            return self.rotate_left(child, child_idx, right_sibling)

        # Otherwise, try  to rotate from left sibling
        elif left_sibling is not None and left_sibling.can_give_up_keys():
            return self.rotate_right(child, child_idx, left_sibling)

        # Otherwise, must merge siblings
        else:
            # if it's last child then merge with left sibling
            if child_idx == self.num_keys():
                self.merge_left(child, left_sibling)

            # otherwise merge with right sibling
            else:
                self.merge_right(child, child_idx, right_sibling)

            # Internal node may now be under because of merge
            # operation - may need to rebalance
            if self.is_deficient():
                return True

    def rotate_left(self, child, child_idx, right_sibling):
        rotate_key = right_sibling.keys[0]
        new_separator = right_sibling.keys[1]

        right_sibling.keys.remove(rotate_key)
        self.keys[child_idx] = new_separator
        if not right_sibling.is_leaf():
            new_child_id = right_sibling.children_ids[0]
            self.children_ids.append(new_child_id)
            del(right_sibling.children_ids[0])

        child.lock.acquire_write()
        child.add_key(rotate_key)

    def rotate_right(self, child, child_idx, left_sibling):
        rotate_key = left_sibling.keys[-1]
        self.keys[child_idx-1] = rotate_key

        left_sibling.keys.remove(rotate_key)
        if not left_sibling.is_leaf():
            new_child_id = left_sibling.children_ids[-1]
            self.children_ids.insert(0, new_child_id)
            del(right_sibling.children_ids[-1])

        child.lock.acquire_write()
        child.add_key(rotate_key)

    def merge_left(self, child, left_sibling):
        # TODO: Change method to be dynamic and not rely on node being
        # last child. Could even leverage merge_right method instead.
        separator = self.keys[-1]

        self.keys.pop()
        if not child.is_leaf():
            left_sibling.keys.append(separator)

        self.children_ids.pop()

        left_sibling.keys = left_sibling.keys + child.keys
        left_sibling.children_ids = left_sibling.children_ids + child.children_ids

    def merge_right(self, child, child_idx, right_sibling):
        separator = self.keys[child_idx]

        self.keys.remove(separator)
        if not child.is_leaf():
            right_sibling.keys.insert(0, separator)

        self.children_ids.remove(child.id)

        right_sibling.keys = child.keys + right_sibling.keys
        right_sibling.children_ids = child.children_ids + right_sibling.children_ids

### build tree
btree = BPlusTree(4)
btree.root = Node([13, 24, 30], 4)
child_one = Node([2, 3, 5, 7], 4)
child_two = Node([14, 16, 19, 22], 4)
child_three = Node([24, 27, 29], 4)
child_four = Node([33, 34, 38, 39], 4)
btree.root.children_ids = [child_one.id, child_two.id, child_three.id, child_four.id]

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
