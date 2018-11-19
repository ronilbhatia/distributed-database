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

class Node:
    def __init__(self, keys = [], max_keys = 2):
        self.keys = keys
        self.max_keys = max_keys
        self.min_keys = max_keys // 2
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
        # Check if node is leaf - attempt to remove key directly
        if self.is_leaf():
            try:
                self.keys.remove(key)
            except:
                print("key does not exist")
                return

            # Check it underflow has occurred and need to rebalance
            if len(self.keys) < self.min_keys:
                return True
        else:
            # if the node is not a leaf we attempt to find the key in
            # the node itself or find the appropriate child where the
            # key should be if it exists in the tree
            for i in range(self.count()):
                found_child = False
                found_key = False
                # Check if key is actually contained in node
                if key == self.keys[i]:
                    # logic for deleting key in internal node - rebalance
                    found_key = True
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
            # from leaf and we must restructure the tree
            if res:
                child = self.children[child_idx]

                # Try to rotate from right child first, if it has
                # enough keys to give up one
                if len(self.children) > (child_idx + 1) and self.children[child_idx+1].count() > self.min_keys:
                    sibling = self.children[child_idx+1]
                    closest_key = sibling.keys[0]
                    rotate_key = self.keys[child_idx]

                    sibling.keys.remove(closest_key)
                    self.keys.remove(rotate_key)
                    self.keys.insert(child_idx, closest_key)
                    child.add_key(rotate_key)

                # Otherwise, try  to rotate from left child
                elif 0 >= (child_idx - 1) and self.children[child_idx-1].count() > self.min_keys:
                        sibling = self.children[child_idx-1]
                        closest_key = sibling.keys[-1]
                        rotate_key = self.keys[child_idx-1]

                        sibling.keys.remove(closest_key)
                        self.keys.remove(rotate_key)
                        self.keys.insert(child_idx, closest_key)
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
                        self.children.pop()

                    # otherwise merge with right sibling
                    else:
                        sibling = self.children[child_idx+1]
                        separator = self.keys[child_idx]

                        sibling.keys.append(separator)
                        self.keys.remove(separator)
                        sibling.keys = sibling.keys + child.keys
                        self.children.remove(child)

btree = BTree(4)
btree.add_key(1)
btree.add_key(2)
btree.add_key(3)
btree.add_key(4)
btree.add_key(5)
btree.add_key(6)
btree.add_key(7)
btree.add_key(8)
btree.add_key(9)
btree.add_key(10)
btree.add_key(11)
btree.add_key(12)
btree.add_key(13)
btree.add_key(14)
btree.add_key(15)
btree.add_key(16)
btree.add_key(17)
pdb.set_trace()
btree.root.remove_key(16)
