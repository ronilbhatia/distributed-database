import pdb

class BTree:
    def __init__(self, max_keys = 2):
        self.root = Node([], max_keys)

    def add_key(self, key):
        res = self.root.add_key(key)
        # pdb.set_trace()
        if res:
            self.root.keys = [res['median']]
            self.root.children = [res['left'], res['right']]
            return res

class Node:
    def __init__(self, keys = [], max_keys = 2):
        self.keys = keys
        self.max_keys = max_keys
        self.children = []

    def count(self):
        return len(self.keys)

    def is_full(self):
        return self.count() == self.max_keys

    def is_leaf(self):
        return len(self.children) == 0

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
                    child = i
                    break
            # if we didn't find a child it must be the last child
            if not found_child:
                res = self.children[-1].add_key(key)
                child = len(self.children) - 1
            # if we have a res that implies the child had to be split
            if res:
                median = res['median']
                left = res['left']
                right = res['right']
                # If the node has space we can add the median value on
                # We will add the left and right nodes as children of
                # the node on either side of the median value
                if not self.is_full():
                    is_inserted = False
                    for i in range(self.count()):
                        if median < self.keys[i]:
                            self.keys.insert(i, median)
                            del self.children[child]
                            self.children.insert(i, left)
                            self.children.insert(i+1, right)
                            is_inserted = True
                            break
                    # if we didn't insert, assume the median value will
                    # fill the last available spot in the node, and same
                    # for the children (left and right nodes)
                    if not is_inserted:
                        self.keys.append(median)
                        del self.children[child]
                        self.children.append(left)
                        self.children.append(right)
                # If the node does not have space we must split it. We
                # must not lose track of the left and right nodes though
                # so we can remove the original child node (since it has
                # been split) and insert the left & right nodes as
                # children, passing the median value to split the node
                else:
                    del self.children[child]
                    self.children.insert(child, left)
                    self.children.insert(child+1, right)
                    return self.split(median)


    def split(self, key):
        # TODO: not efficient, improve later
        # pdb.set_trace()
        sorted_keys = self.keys
        sorted_keys.append(key)
        sorted_keys.sort()
        mid_idx = (self.max_keys + 1)//2

        # split up the node into left, right, and median
        median = sorted_keys[mid_idx]
        left = Node(sorted_keys[0:mid_idx], self.max_keys)
        left.children = self.children[0:mid_idx+1]
        right = Node(sorted_keys[mid_idx+1:], self.max_keys)
        right.children = self.children[mid_idx+1:]
        return {'median': median, 'left': left, 'right': right}

    def remove_key(self, key):
        return

node = BTree()
# node.add_key(7)
# node.add_key(6)
# node.add_key(5)
# node.add_key(4)
# node.add_key(3)
# node.add_key(2)
# node.add_key(4)
# node.add_key(1)
# node.add_key(5)
# node.add_key(2)
# node.add_key(10)
# node.add_key(11)
# # node.add_key(11)
# node.add_key(3)
# node.add_key(6)
# node.add_key(9)
# node.add_key(7)
# node.add_key(8)
node.add_key(1)
node.add_key(2)
node.add_key(3)
node.add_key(4)
node.add_key(5)
node.add_key(6)
node.add_key(7)

# pdb.set_trace()
