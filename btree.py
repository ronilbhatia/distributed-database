class BTree:
    def __init__(self, max_keys = 2):
        self.root = Node(max_keys)

    def add_key(self, key):
        return

class Node:
    def __init__(self, keys = [], max_keys = 2):
        self.keys = keys
        self.max_keys = max_keys

    def is_full(self):
        return len(self.keys) == self.max_keys

    def add_key(self, key):
        if not self.is_full:
            is_inserted = False
            for i in range(self.max_keys):
                if key < self.keys[i]:
                    self.keys.insert(i, key)
                    is_inserted = True
                    break
            if not is_inserted:
                self.keys.append(key)
        else:
            self.split()

    def split(self, key):
        # TODO: not efficient, improve later
        sorted_keys = self.keys.append(key).sort()
        mid_idx = (self.max_keys + 1)//2

        # split up the
        median = sorted_keys[mid_idx]
        left = Node(sorted_keys[0:mid_idx], self.max_keys)
        right = Node(sorted_keys[mid_idx+1:], self.max_keys)

        return {'median': median, 'left': left, 'right': right}

node = Node()
print(node.max_keys)
