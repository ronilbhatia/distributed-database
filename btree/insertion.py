import pdb

class Insertion:
    def add_key(self, key):
        if self.is_leaf():
            return self.add_key_leaf(key)
        else:
            return self.add_key_internal(key)


    def add_key_leaf(self, key):
        key_idx = self.find_idx(key)
        self.keys.insert(key_idx, key)

        # if the node is overflowed we need to split it
        if self.overflow():
            return self.split()

    def add_key_internal(self, key):
        children = self.get_children()

        # if the node is not a leaf we must find the appropriate
        # child to attempt to add the key to
        child_idx = self.find_idx(key)
        child = children[child_idx]

        split_info = children[child_idx].add_key(key)

        # if we have split_info that implies the child had to be split
        if split_info:
            return self.handle_split(split_info, child_idx)

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

        left = self.__class__(left_keys, max_keys, left_children_ids)
        right = self.__class__(right_keys, max_keys, right_children_ids)

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

        # If the node is overflowed we must split it.
        if self.overflow():
            return self.split()
