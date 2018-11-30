import pdb
import uuid

class BTree:
    def __init__(self, max_keys = 2):
        self.root = Node([], max_keys)
        self.num_keys = 0
        self.min_keys = max_keys // 2

    def add_key(self, key):
        self.num_keys += 1
        res = self.root.add_key(key)

        if res:
            self.root.keys = [res['median']]
            self.root.children_ids = [res['left_id'], res['right_id']]
            return res

    def remove_key(self, key):
        self.num_keys -= 1
        self.root.remove_key(key)

        if self.root.is_empty():
            self.root = Node.get_node(self.root.children_ids[0])

class Node:
    # Class variable to store all nodes
    nodes = {}

    @classmethod
    def get_node(self, id):
        return self.nodes.get(id)

    def __init__(self, keys = [], max_keys = 2):
        self.keys = keys
        self.max_keys = max_keys
        self.min_keys = max_keys // 2
        self.id = uuid.uuid4()

        # Add node to hash map, ensuring it has a unique id
        while Node.get_node(self.id) is not None:
            self.id = uuid.uuid4()
        Node.nodes[self.id] = self

        self.children_ids = []


    # see if these methods would be helpful
    def get_child_at_index(self, child_idx):
        if child_idx < 0 or child_idx >= len(self.children_ids):
            return None
        else:
            return Node.get_node(self.children_ids[child_idx])

    def set_child_at_index(self):
        pass

    # gives a list comprehension, taking each child id and looking up in
    # hash map
    def get_children(self):
        children = [Node.get_node(id) for id in self.children_ids]
        return children

    def count(self):
        return len(self.keys)

    def is_full(self):
        return self.count() == self.max_keys

    def overflow(self):
        return self.count() > self.max_keys

    def is_empty(self):
        return self.count() == 0

    def is_leaf(self):
        return len(self.children_ids) == 0

    def can_give_up_keys(self):
        return self.min_keys < self.count()

    def is_deficient(self):
        return self.count() < self.min_keys

    def add_key(self, key):
        # Check if node is leaf - if so attempt to add
        if self.is_leaf():
            return self.add_key_leaf(key)
        else:
            return self.add_key_internal(key)

    def add_key_leaf(self, key):
        is_inserted = False
        for i in range(self.count()):
            if key < self.keys[i]:
                self.keys.insert(i, key)
                is_inserted = True
                break
            elif key == self.keys[i]:
                print("key already exists")
                return
        if not is_inserted:
            self.keys.append(key)
        # if the node is overflowed we need to split it
        if self.overflow():
            return self.split()

    def add_key_internal(self, key):
        children = self.get_children()
        # if the node is not a leaf we must find the appropriate
        # child to attempt to add the key to
        found_child = False
        for i in range(self.count()):
            if key < self.keys[i]:
                found_child = True
                res = children[i].add_key(key)
                child_idx = i
                break
            elif key == self.keys[i]:
                print("key already exists")
                return
        # if we didn't find a child it must be the last child
        if not found_child:
            res = children[-1].add_key(key)
            child_idx = len(children) - 1
        # if we have a res that implies the child had to be split
        if res:
            return self.restructure(res, child_idx)

    def split(self):
        mid_idx = (self.max_keys + 1)//2

        # split up the node into left, right, and median
        median = self.keys[mid_idx]
        left = Node(self.keys[0:mid_idx], self.max_keys)
        left.children_ids = self.children_ids[0:mid_idx+1]
        right = Node(self.keys[mid_idx+1:], self.max_keys)
        right.children_ids = self.children_ids[mid_idx+1:]

        return {'median': median, 'left_id': left.id, 'right_id': right.id}

    def restructure(self, res, child_idx):
        median = res['median']
        left_id = res['left_id']
        right_id = res['right_id']

        # We will add the left and right nodes as children of
        # the node on either side of the median value regardless
        # of whether the node is full or not and must be split.
        del self.children_ids[child_idx]
        self.children_ids.insert(child_idx, left_id)
        self.children_ids.insert(child_idx + 1, right_id)
        self.keys.insert(child_idx, median)

        # If the node is overflowed we must split it.
        if self.overflow():
            return self.split()

    def find_left_leaf(self, i):
        # initially set leaf to left child
        curr_node = self.get_child_at_index(i)

        # continue to reassign leaf to right-most child until the node
        # has no children (i.e. it is a leaf)
        while len(curr_node.children_ids) > 0:
            curr_node = Node.get_node(curr_node.children_ids[-1])

        return curr_node

    def find_right_leaf(self, i):
        # initially set leaf to right child
        curr_node = self.get_child_at_index(i+1)

        # continue to reassign leaf to left-most child until the node
        # has no children (i.e. it is a leaf)
        while len(curr_node.children_ids) > 0:
            curr_node = Node.get_node(curr_node.children_ids[0])

        return curr_node

    def remove_key(self, key):
        # Check if node is leaf - attempt to remove key directly
        if self.is_leaf():
            try:
                self.keys.remove(key)
            except:
                print("key does not exist")
                return

            # Check it underflow has occurred and need to rebalance
            if self.count() < self.min_keys:
                return True
        else:
            children = self.get_children()
            found_child = False
            found_key = False
            # if the node is not a leaf we attempt to find the key in
            # the node itself or find the appropriate child where the
            # key should be if it exists in the tree
            for i in range(self.count()):
                # Check if key is actually contained in node
                if key == self.keys[i]:
                    # logic for deleting key in internal node
                    found_key = True
                    self.keys.remove(key)

                    left_leaf = self.find_left_leaf(i)
                    right_leaf = self.find_right_leaf(i)

                    # Take from left leaf if it can give up keys
                    if left_leaf.can_give_up_keys():
                        new_separator = left_leaf.keys[-1]
                        self.keys.insert(i, new_separator)
                        child_idx = i

                        # Even though we've found the key in the leaf,
                        # use our method on the current node we are on
                        # incase recursive rebalancing is necessary
                        res = children[i].remove_key(new_separator)

                    # Otherwise, take from right leaf
                    else:
                        new_separator = right_leaf.keys[0]
                        self.keys.insert(i, new_separator)
                        child_idx = i+1
                        res = children[i+1].remove_key(new_separator)
                    break

                # If we didn't find the key yet, and it's less than the
                # current element, then it should live in the subtree of
                # this child
                elif key < self.keys[i]:
                    found_child = True
                    res = children[i].remove_key(key)
                    child_idx = i
                    break

            # If we still didn't find it, it's in the last child's subtree
            if not found_child and not found_key:
                res = children[-1].remove_key(key)
                child_idx = len(children) - 1

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
            closest_key = right_sibling.keys[0]
            rotate_key = self.keys[child_idx]

            right_sibling.keys.remove(closest_key)
            self.keys.remove(rotate_key)
            self.keys.insert(child_idx, closest_key)
            # TODO:  Make sure to move children_ids appropriately as well
            child.add_key(rotate_key)

        # Otherwise, try  to rotate from left sibling
        elif left_sibling is not None and left_sibling.can_give_up_keys():
            closest_key = left_sibling.keys[-1]
            rotate_key = self.keys[child_idx-1]

            left_sibling.keys.remove(closest_key)
            self.keys.remove(rotate_key)
            self.keys.insert(child_idx-1, closest_key)
            # TODO:  Make sure to move children_ids appropriately as well
            child.add_key(rotate_key)

        # Otherwise, must merge siblings
        else:
            # if it's last child then merge with left sibling
            if child_idx == self.count():
                separator = self.keys[-1]

                left_sibling.keys.append(separator)
                self.keys.pop()
                left_sibling.keys = left_sibling.keys + child.keys
                left_sibling.children_ids = left_sibling.children_ids + child.children_ids
                self.children_ids.pop()

            # otherwise merge with right sibling
            else:
                separator = self.keys[child_idx]

                right_sibling.keys.insert(0, separator)
                self.keys.remove(separator)
                right_sibling.keys = child.keys + right_sibling.keys
                right_sibling.children_ids = child.children_ids + right_sibling.children_ids
                self.children_ids.remove(child.id)

            # Internal node may now be under because of merge
            # operation - may need to rebalance
            if self.is_deficient():
                return True

btree = BTree(2)
btree.add_key(1)
btree.add_key(2)
btree.add_key(3)
btree.add_key(4)
btree.add_key(5)
btree.add_key(5)
pdb.set_trace()
# btree.add_key(6)
# btree.add_key(7)
# btree.add_key(8)
# btree.add_key(9)
# btree.add_key(10)
# btree.add_key(11)
# btree.add_key(12)
# btree.add_key(13)
# btree.add_key(14)
# btree.add_key(15)
# btree.add_key(16)
# btree.add_key(17)
# btree.root.remove_key(11)
# btree = BTree(5)
# btree.add_key('A')
# btree.add_key('B')
# btree.add_key('C')
# btree.add_key('D')
# btree.add_key('E')
# btree.add_key('F')
# btree.add_key('G')
# btree.add_key('H')
# btree.add_key('I')
# btree.add_key('J')
# btree.add_key('K')
# btree.add_key('L')
# btree.add_key('M')
# btree.add_key('N')
# btree.add_key('O')
# btree.add_key('P')
# btree.add_key('Q')
# btree.add_key('R')
# btree.add_key('S')
# btree.add_key('T')
# btree.add_key('U')
# btree.add_key('V')
# btree.add_key('W')
# btree.add_key('X')
# btree.add_key('Y')
# btree.add_key('Z')
# btree.remove_key('G')
# btree.remove_key('L')
# btree.remove_key('M')
# btree.remove_key('H')
# btree.remove_key('Z')
# btree.remove_key('U')
# btree.remove_key('N')
# btree.remove_key('O')
# btree.remove_key('F')
# btree.remove_key('J')
pdb.set_trace()
