import pdb

class BTree:
    def __init__(self, max_keys = 2):
        self.root = Node([], max_keys)
        self.num_keys = 0
        self.min_keys = max_keys // 2

    def add_key(self, key):
        self.num_keys += 1
        res = self.root.add_key(key)
        # pdb.set_trace()
        if res:
            self.root.keys = [res['median']]
            self.root.children = [res['left'], res['right']]
            return res

    def remove_key(self, key):
        self.num_keys -= 1
        self.root.remove_key(key)

        if self.root.is_empty():
            pdb.set_trace()
            self.root = self.root.children[0]

class Node:
    def __init__(self, keys = [], max_keys = 2):
        self.keys = keys
        self.max_keys = max_keys
        self.min_keys = max_keys // 2
        # TODO: Look up how to generate random string in python
        self.identifier = 2
        self.children = []
        # self.children_identifiers = []
        # TODO:  define a class variable with hashmap to store all nodes, with
        # keys as identifiers, and values as nodes

    # TODO: this needs to be a global method - lookup how to do that
    @classmethod
    def get_node(identifier):
        # key into hash map at identifer
        pass

    # see if these methods would be helpful
    def get_child_at_index(self):
        pass

    def set_child_at_index(self):
        pass

    # gives a list comprehension, taking each child id and looking up in
    # hash map
    def children(self):
        pass

    def count(self):
        return len(self.keys)

    def is_full(self):
        return self.count() == self.max_keys

    def is_empty(self):
        return self.count() == 0

    def is_leaf(self):
        return len(self.children) == 0

    def can_give_up_keys(self):
        return self.min_keys < self.count()

    def is_deficient(self):
        return self.count() < self.min_keys

    def add_key(self, key):
        # Check if node is leaf - if so attempt to add
        if self.is_leaf():
            # Check if node is full - if not add key directly on
            if not self.is_full():
                is_inserted = False
                for i in range(self.count()):
                    if key < self.keys[i]:
                        self.keys.insert(i, key)
                        is_inserted = True
                        break
                if not is_inserted:
                    self.keys.append(key)
            # if the node is full we need to split it
            else:
                return self.split(key)
        else:
            # if the node is not a leaf we must find the appropriate
            # child to attempt to add the key to
            for i in range(self.count()):
                found_child = False
                if key < self.keys[i]:
                    found_child = True
                    res = self.children[i].add_key(key)
                    child_idx = i
                    break
            # if we didn't find a child it must be the last child
            if not found_child:
                res = self.children[-1].add_key(key)
                child_idx = len(self.children) - 1
            # if we have a res that implies the child had to be split
            if res:
                median = res['median']
                left = res['left']
                right = res['right']

                # We will add the left and right nodes as children of
                # the node on either side of the median value regardless
                # of whether the node is full or not and must be split.
                del self.children[child_idx]
                self.children.insert(child_idx, left)
                self.children.insert(child_idx + 1, right)

                # If the node has space we can add the median value on
                if not self.is_full():
                    self.keys.insert(child_idx, median)

                # If the node does not have space we must split it.
                else:
                    return self.split(median)


    def split(self, key):
        # Insert key into node in correct spot
        for i in range(self.count()):
            found_spot = False
            if key < self.keys[i]:
                found_spot = True
                self.keys.insert(i, key)
                break

        # if we didn't find a spot it must go on the end
        if not found_spot:
            self.keys.append(key)

        mid_idx = (self.max_keys + 1)//2

        # split up the node into left, right, and median
        median = self.keys[mid_idx]
        left = Node(self.keys[0:mid_idx], self.max_keys)
        left.children = self.children[0:mid_idx+1]
        right = Node(self.keys[mid_idx+1:], self.max_keys)
        right.children = self.children[mid_idx+1:]
        return {'median': median, 'left': left, 'right': right}

    def find_left_leaf(self, i):
        # initially set leaf to left child
        curr_node = self.children[i]

        # continue to reassign leaf to right-most child until the node
        # has no children (i.e. it is a leaf)
        while len(curr_node.children) > 0:
            curr_node = curr_node.children[-1]

        return curr_node

    def find_right_leaf(self, i):
        # initially set leaf to right child
        curr_node = self.children[i+1]

        # continue to reassign leaf to left-most child until the node
        # has no children (i.e. it is a leaf)
        while len(curr_node.children) > 0:
            curr_node = curr_node.children[0]

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
                        res = self.children[i].remove_key(new_separator)

                    # Otherwise, take from right leaf
                    else:
                        new_separator = right_leaf.keys[0]
                        self.keys.insert(i, new_separator)
                        child_idx = i+1
                        # pdb.set_trace()
                        res = self.children[i+1].remove_key(new_separator)
                    break

                # If we didn't find the key yet, and it's less than the
                # current element, then it must be in a child node if it
                # exists in the tree at all
                elif key < self.keys[i]:
                    found_child = True
                    res = self.children[i].remove_key(key)
                    child_idx = i
                    break

            # If we still didn't find it, it's in the last child
            if not found_child and not found_key:
                res = self.children[-1].remove_key(key)
                child_idx = len(self.children) - 1

            # having a res implies underflow occurred when removing key
            # from child and we must restructure the tree
            if res:
                child = self.children[child_idx]

                # Try to rotate from right sibling first, if it has
                # enough keys to give up one
                if len(self.children) > (child_idx + 1) and self.children[child_idx+1].can_give_up_keys():
                    sibling = self.children[child_idx+1]
                    closest_key = sibling.keys[0]
                    rotate_key = self.keys[child_idx]

                    sibling.keys.remove(closest_key)
                    self.keys.remove(rotate_key)
                    self.keys.insert(child_idx, closest_key)
                    child.add_key(rotate_key)

                # Otherwise, try  to rotate from left child
                elif (child_idx - 1) >= 0 and self.children[child_idx-1].can_give_up_keys():
                        sibling = self.children[child_idx-1]
                        closest_key = sibling.keys[-1]
                        rotate_key = self.keys[child_idx-1]

                        sibling.keys.remove(closest_key)
                        self.keys.remove(rotate_key)
                        self.keys.insert(child_idx-1, closest_key)
                        child.add_key(rotate_key)

                # Otherwise, must merge siblings
                else:
                    # if it's last child then merge with left sibling
                    if child_idx == self.count():
                        sibling = self.children[child_idx-1]
                        separator = self.keys[-1]

                        sibling.keys.append(separator)
                        self.keys.pop()
                        sibling.keys = sibling.keys + child.keys
                        sibling.children = sibling.children + child.children
                        self.children.pop()

                        # reassign child_idx to use in case parent has
                        # gone through underflow
                        child_idx -= 1

                    # otherwise merge with right sibling
                    else:
                        sibling = self.children[child_idx+1]
                        separator = self.keys[child_idx]

                        sibling.keys.insert(0, separator)
                        self.keys.remove(separator)
                        sibling.keys = child.keys + sibling.keys
                        sibling.children = child.children + sibling.children
                        self.children.remove(child)

                    # Internal node may now be under because of merge
                    # operation - may need to rebalance
                    if self.is_deficient():
                        return True
# btree = BTree(4)
# btree.add_key(1)
# btree.add_key(2)
# btree.add_key(3)
# btree.add_key(4)
# btree.add_key(5)
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
btree = BTree(5)
btree.add_key('A')
btree.add_key('B')
btree.add_key('C')
btree.add_key('D')
btree.add_key('E')
btree.add_key('F')
btree.add_key('G')
btree.add_key('H')
btree.add_key('I')
btree.add_key('J')
btree.add_key('K')
btree.add_key('L')
btree.add_key('M')
btree.add_key('N')
btree.add_key('O')
btree.add_key('P')
btree.add_key('Q')
btree.add_key('R')
btree.add_key('S')
btree.add_key('T')
btree.add_key('U')
btree.add_key('V')
btree.add_key('W')
btree.add_key('X')
btree.add_key('Y')
btree.add_key('Z')
btree.remove_key('G')
btree.remove_key('L')
btree.remove_key('M')
btree.remove_key('H')
btree.remove_key('Z')
btree.remove_key('U')
btree.remove_key('N')
btree.remove_key('O')
btree.remove_key('F')
btree.remove_key('J')
pdb.set_trace()
