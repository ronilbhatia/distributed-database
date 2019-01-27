import pdb

class Insertion:
    def add_key(self, key):
        if self.is_leaf():
            return self.add_key_leaf(key)
        else:
            return self.add_key_internal(key)
                
    def add_key_leaf(self, key):
        self.acquire_write()

        if self.is_not_rightmost() and key > self.max_key:
            self.release_write()
            new_node = self.scan_right_for_write_guard(key)
            return new_node.add_key_leaf(key)

        key_idx = self.find_idx(key)
        self.keys.insert(key_idx, key)
    
        # if the node is overflowed we need to split it
        if self.overflow():
            return self.split()
        else:
            self.release_write()
                

    def add_key_internal(self, key):
        # Need to lock self before reading anything
        self.acquire_read()

        children = self.get_children()

        # This node might have been split by the time we reach it, and no longer
        # be the appropriate sub-tree where the key exists, so we scan right
        if self.is_not_rightmost() and key > self.max_key:
            new_node = self.scan_right_for_write_guard(key)
            self.release_read()

            # Unlock node for reading before doing this
            return new_node.add_key_internal(key)

        # if the node is not a leaf we must find the appropriate
        # child to attempt to add the key to
        child_idx = self.find_idx(key)
        child = children[child_idx]

        # Once appropriate child has been found, can release the read lock
        self.release_read()

        split_info = child.add_key(key)

        # if we have split_info that implies the child had to be split
        if split_info:
            node = self
            node.acquire_write()

            while node.is_not_rightmost() and key > node.max_key:
                new_node = node.scan_right_for_write_guard(key)
                node.release_write()
                new_node.acquire_write()
                node = new_node
            
            # Find child_idx again because could be at a different node
            child_idx = node.find_idx(key)
            return node.handle_split(split_info, child_idx)

    def split(self):
        if not self.locked():
            # Raise exception - record which thread locked it
            raise "Node is not locked"

        mid_idx = self.num_keys()//2

        # split up the node into left, right, and median
        median = self.keys[mid_idx]
        max_keys = self.max_keys
        max_key = self.max_key

        left_keys = self.keys[0:mid_idx]
        left_children_ids = self.children_ids[0:mid_idx+1]

        # If node is leaf, we want to keep the median value in the right
        # node as well, but if it is not then we don't
        if self.is_leaf():
            right_keys = self.keys[mid_idx:]
        else:
            right_keys = self.keys[mid_idx+1:]
        right_children_ids = self.children_ids[mid_idx+1:]

        # The node turns into the left node of the split, and a new node
        # is created for the right node, that we link it to
        self.keys = left_keys
        self.children_ids = left_children_ids

        right = self.__class__(right_keys, max_keys, max_key, right_children_ids)

        # The right node now has this node's link and this node acquires a link to the right node
        right.link = self.link
        self.link = right.id

        # left child's (self's) max key is the separator value
        self.max_key = median

        # Release write lock on self
        self.release_write()

        return {'median': median, 'left_id': self.id, 'right_id': right.id}

    def handle_split(self, split_info, child_idx):
        if not self.locked():
            # Raise exception - record which thread locked it
            raise "Node is not locked"

        median = split_info['median']
        left_id = split_info['left_id']
        right_id = split_info['right_id']

        # We will add the left and right nodes as children of
        # the node on either side of the median value regardless
        # of whether the node is full or not and must be split.

        # Check that left_id is equal to the id already at children_ids[child_idx]
        if not self.children_ids[child_idx] == left_id:
            raise "Not at the correct parent node"
            
        self.children_ids.insert(child_idx + 1, right_id)
        self.keys.insert(child_idx, median)

        # If the node is overflowed we must split it.
        if self.overflow():
            return self.split()
        else:
            self.release_write()


# def add_key_root(self, key):
#         if self.is_leaf():
#             self.acquire_write()
#             print("Locked root with keys ", self.keys)

#             if self.is_not_rightmost() and key > self.max_key:
#                 self.release_write()
#                 new_node = self.scan_right_for_write_guard(key)
#                 return new_node.add_key_root(key)

#             key_idx = self.find_idx(key)
#             self.keys.insert(key_idx, key)

#             if self.overflow():
#                 return {'split_info': 'leaf overflow', 'node': self, 'child_idx': None}
#             else:
#                 self.release_write()
#                 print("Unlocked root")
#                 return {'split_info': None, 'child_idx': None}
#         else:
#             children = self.get_children()

#             # if the node is not a leaf we must find the appropriate
#             # child to attempt to add the key to

#             #TODO Add read lock
#             child_idx = self.find_idx(key)
#             child = children[child_idx]

#             split_info = children[child_idx].add_key(key)

#             return {'node': self, 'split_info': split_info, 'child_idx': child_idx}
