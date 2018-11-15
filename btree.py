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
        if self.is_leaf():
            if not self.is_full():
                is_inserted = False
                for i in range(self.count()):
                    if key < self.keys[i]:
                        self.keys.insert(i, key)
                        is_inserted = True
                        break
                if not is_inserted:
                    self.keys.append(key)
            else:
                return self.split(key)
        else:
            for i in range(self.count()):
                found_child = False
                if key < self.keys[i]:
                    found_child = True
                    res = self.children[i].add_key(key)
                    child = i
                    break
            if not found_child:
                res = self.children[-1].add_key(key)
                child = len(self.children) - 1
            if res:
                median = res['median']
                left = res['left']
                right = res['right']
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
                    if not is_inserted:
                        self.keys.append(median)
                        del self.children[child]
                        self.children.append(left)
                        self.children.append(right)
                else:
                    # child = self.children[child]
                    # child.keys = [median]
                    # child.children = [left, right]
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

node = BTree()
node.add_key(7)
node.add_key(6)
node.add_key(5)
node.add_key(4)
node.add_key(3)
node.add_key(2)
node.add_key(1)
# node.add_key(2)
# node.add_key(3)
# node.add_key(4)
# node.add_key(5)
# node.add_key(6)
# node.add_key(7)

pdb.set_trace()
print(node.count())
print(node.keys, node.children)
