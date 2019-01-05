import pdb

class Insertion:
    def find_parent_of_deepest_stable_ancestor(self, key):
        curr_node = self
        parent = self
        deepest_stable_ancestor = self

        while not curr_node.is_leaf():
            child_idx = curr_node.find_idx(key)
            child_id = curr_node.children_ids[child_id]
            child = self.__class__.get_node(child_id)

            if child.is_stable():
                parent = curr_node
                deepest_stable_ancestor = child

            curr_node = child

    def add_key(self, key, lock_path = []):
        if self.is_leaf():
            return self.add_key_leaf(key, lock_path)
        else:
            return self.add_key_internal(key, lock_path)


    def add_key_leaf(self, key, lock_path):
        key_idx = self.find_idx(key)

        # If leaf is not stable, must acquire write locks all the way
        # up lock path through first stable node or root if there is
        # no node is stable
        if not self.is_stable():
            lock_path.acquire_writes()
        else:
            print("Releasing read lock and acquiring write lock on leaf with keys ", self.keys)
            self.release_read()
            self.acquire_write()

        self.keys.insert(key_idx, key)

        # if the node is overflowed we need to split it
        if self.overflow():
            return self.split()
        else:
            print("Releasing write lock for leaf with keys ", self.keys)
            self.release_write()
            # Release read lock on parent (only two nodes in lock path
            # are leaf and its parent)
            print("Releasing read lock for node with keys", lock_path[0].keys)
            lock_path[0].release_read()

    def add_key_internal(self, key, lock_path):
        children = self.get_children()

        # if the node is not a leaf we must find the appropriate
        # child to attempt to add the key to
        child_idx = self.find_idx(key)
        child = children[child_idx]

        print("Acquiring read lock on node with keys ", child.keys)
        child.lock.acquire_read()

        # Release read locks on everything except self if child is stable
        if child.is_stable():
            for idx, node in enumerate(lock_path[0:-1]):
                print("Releasing read lock on node with keys ", node.keys)
                node.release_read()

            lock_path = [lock_path[-1]]

        lock_path.append(child)
        split_info = children[child_idx].add_key(key, lock_path)

        # if we have split_info that implies the child had to be split
        if split_info:
            return self.handle_split(split_info, child_idx, lock_path)

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

    def handle_split(self, split_info, child_idx, lock_path):
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
        print("Releasing write lock on orphaned node with keys ", orphan.keys)
        orphan.release_write()

        # If the node is overflowed we must split it.
        if self.overflow():
            return self.split()
        else:
            print("Releasing write lock on node with keys ", self.keys)
            print(self.keys, lock_path, self.lock.locked())
            self.release_write()

            # If node doesn't overflow it means it was stable, and everything
            # before it is read-locked. The last two nodes in the lock path
            # would be itself, and its child who were write locked, so we do
            # not release read locks on them.
            for idx, node in enumerate(lock_path[0:-2]):
                print("Releasing read lock on node with keys ", node.keys)
                node.release_read()
