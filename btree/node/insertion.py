from .base import Base

class Insertion:
    def add_key(self, key):
        self.assert_is_unlocked_by_current_thread()
        try:
            self.acquire_read()
            is_leaf = self.is_leaf()
            self.release_read()

            if is_leaf:
                return self.add_key_leaf(key)
            else:
                return self.add_key_internal(key)
        finally:
            self.assert_is_unlocked_by_current_thread()

    def add_key_leaf(self, key):
        self.assert_is_unlocked_by_current_thread()
        try:
            self.acquire_write()

            if self.is_not_rightmost() and self.max_key_is_smaller_than(key):
                self.release_write()
                self.acquire_read()
                new_node = self.scan_right_for_read_guard(key)
                new_node.release_read()
                return new_node.add_key_leaf(key)

            key_idx = self.find_child_idx(key)
            # NR: We must add a helper to only allow this if we have a write
            # lock.
            # RB: Updated
            self.insert_key(key_idx, key)

            # if the node is overflowed we need to split it
            if self.overflow():
                split_info = self.split()
            else:
                split_info = None
            
            self.release_write()
            return split_info
        finally:
            self.assert_is_unlocked_by_current_thread()

    def add_key_internal(self, key):
        self.assert_is_unlocked_by_current_thread()
        try:
            # Need to lock self before reading anything
            self.acquire_read()
            children = self.get_children()
            # This node might have been split by the time we reach it, and no longer
            # be the appropriate sub-tree where the key exists, so we scan right
            if self.is_not_rightmost() and self.max_key_is_smaller_than(key):
                print("I'm not the right node, I have keys: ", self.get_keys(), "my max key is: ", self.get_max_key(), "but I'm trying to add the key: ", key)
                new_node = self.scan_right_for_read_guard(key)
                # Unlock node for reading before doing this
                new_node.release_read()
                return new_node.add_key_internal(key)
            
            # if the node is not a leaf we must find the appropriate
            # child to attempt to add the key to
            child_idx = self.find_child_idx(key)
            child = children[child_idx]
            # Once appropriate child has been found, can release the read lock
            self.release_read()

            split_info = child.add_key(key)
            # if we have split_info that implies the child had to be split
            if split_info:
                node = self
                node.assert_is_unlocked_by_current_thread()
                try:
                    node.acquire_write()

                    while node.is_not_rightmost() and node.max_key_is_smaller_than(key):
                        node = node.scan_right_for_write_guard(key)

                    # Find child_idx again because could be at a different node
                    child_idx = node.find_child_idx(key)
                    split_info = node.handle_split(split_info, child_idx)

                    return split_info
                finally:
                    node.assert_is_unlocked_by_current_thread()
        finally:
            self.assert_is_unlocked_by_current_thread()

    def split(self):
        self.assert_is_write_locked_by_current_thread()
        try:
            mid_idx = self.num_keys()//2

            # split up the node into left, right, and median
            median = self.get_keys()[mid_idx]
            max_keys = self.get_max_keys()
            max_key = self.get_max_key()

            left_keys = self.get_keys()[0:mid_idx]
            left_children_ids = self.get_children_ids()[0:mid_idx+1]

            # If node is leaf, we want to keep the median value in the right
            # node as well, but if it is not then we don't
            if self.is_leaf():
                right_keys = self.get_keys()[mid_idx:]
            else:
                right_keys = self.get_keys()[mid_idx+1:]
            right_children_ids = self.get_children_ids()[mid_idx+1:]

            # The node turns into the left node of the split, and a new node
            # is created for the right node, that we link it to
            self.set_keys(left_keys)
            self.set_children_ids(left_children_ids)

            right = self.__class__(right_keys, max_keys, max_key, right_children_ids)
            right.acquire_write()

            # The right node now has this node's link and this node acquires a link to the right node
            right.set_link(self.get_link())
            self.set_link(right.get_id())

            # left child's (self's) max key is the separator value
            self.set_max_key(median)

            # Release write lock on self
            # self.release_write()
            right.release_write()

            return {'median': median, 'left_id': self.get_id(), 'right_id': right.get_id()}
        finally:
            self.assert_is_locked_by_current_thread()
            right.assert_is_unlocked_by_current_thread()

    def handle_split(self, split_info, child_idx):
        self.assert_is_write_locked_by_current_thread()
        try:
            median = split_info['median']
            left_id = split_info['left_id']
            right_id = split_info['right_id']

            # We will add the left and right nodes as children of
            # the node on either side of the median value regardless
            # of whether the node is full or not and must be split.

            # Check that left_id is equal to the id already at children_ids[child_idx]
            if not self.get_children_ids()[child_idx] == left_id:
                print('--------------SELF-------------')
                print('children_ids: ', self.get_children_ids())
                print('child_idx: ', child_idx)
                print('median: ', median)
                print('my keys: ', self.get_keys())

                left_node = Base.get_node(left_id)
                left_node.acquire_read()
                print('--------------LEFT-------------')
                print('id: ', left_id)
                print('my keys: ', left_node.get_keys())
                left_node.release_read()

                right_node = Base.get_node(right_id)
                right_node.acquire_read()
                print('--------------RIGHT-------------')
                print('id: ', right_id)
                print('my keys: ', right_node.get_keys())
                right_node.release_read()

                # raise "Not at the correct parent node"

            # NR: These insert calls don't check whether we have the lock.
            # Should add a method for them.
            # RB: Updated
            self.insert_child_id(child_idx + 1, right_id)
            self.insert_key(child_idx, median)

            # If the node is overflowed we must split it.
            if self.overflow():
                split_info = self.split()
            else:
                split_info = None 
            
            self.release_write()
            return split_info
        finally:
            self.assert_is_unlocked_by_current_thread()
    
    def insert_key(self, idx, key):
        self.assert_is_write_locked_by_current_thread()
        try:
            self.get_keys().insert(idx, key)
        finally:
            self.assert_is_write_locked_by_current_thread()

    def insert_child_id(self, idx, child_id):
        self.assert_is_write_locked_by_current_thread()
        try:
            self.get_children_ids().insert(idx, child_id)
        finally:
            self.assert_is_write_locked_by_current_thread()