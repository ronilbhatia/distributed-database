from .node import Node

class BTree:
    def __init__(self, max_keys = 2):
        # Create root node, and set up root identifier
        self.set_root(max_keys)
        self.num_keys = 0
        self.min_keys = max_keys // 2

    def get_root(self):
        return Node.get_node(self.root_id)

    def set_root(self, max_keys):
        initial_root = Node([], max_keys)
        self.root_id = initial_root.get_id()

    def build_new_root(self, old_root, split_info):
        old_root.assert_is_write_locked_by_current_thread()
        keys = [split_info['median']]
        max_keys = old_root.get_max_keys()

        right_node = Node.get_node(split_info['right_id'])
        # NR: I think you didn't have a call to lock the right_node
        # here.
        right_node.acquire_read()
        max_key = right_node.get_max_key()
        right_node.release_read()
        children_ids = [split_info['left_id'], split_info['right_id']]

        # Create new root
        new_root = Node(keys, max_keys, max_key, children_ids)
        self.root_id = new_root.get_id()

        # Release lock on old root
        old_root.release_write()

        old_root.assert_is_unlocked_by_current_thread()
        new_root.assert_is_unlocked_by_current_thread()

    def add_key(self, key):
        print("Adding key ", key)
        self.num_keys += 1

        root = self.get_root()
        split_info = root.add_key(key)

        if split_info:
            root.acquire_write()

            if self.root_id == root.get_id():
                self.build_new_root(root, split_info)
            else:
                # The root is no longer the root. We must re-descend to the node
                # we last split from the new root, and potentially by the time
                # we finish doing that the new root will again no longer be the root
                # Thus, this process is a loop.
                while self.root_id != root.get_id():
                    last_node = root
                    last_node.release_write()
                    last_node.acquire_read()
                    # Since the root was split, it's possible that it isn't even
                    # the node we want to continue propagating up from
                    while last_node.is_not_rightmost() and last_node.max_key_is_smaller_than(key):
                        last_node = last_node.scan_right_for_read_guard(key)

                    # Redescend from new root node to find path to last node
                    path = self.find_path_from_new_root(last_node)
                    root = path.pop()

                    for node in path:
                        node.acquire_write()

                        # Again, make sure this is the correct node to handle split
                        while node.is_not_rightmost() and key > node.max_key:
                            node.release_write()
                            new_node = node.scan_right_for_write_guard(key)
                            node = new_node

                        child_idx = node.find_child_idx(key)

                        # handle_split will take care of releasing write lock
                        split_info = node.handle_split(split_info, child_idx)

                        # If there is no split_info, there is no further propagation required
                        if not split_info:
                            return

                    root.acquire_write()
                    if self.root_id == root.get_id():
                        return self.build_new_root(root, split_info)

    def find_path_from_new_root(self, last_node):
        last_node.assert_is_locked_by_current_thread()

        try:
            new_root = self.get_root()
            new_root.acquire_read()
            # By the time we acquire read, root might have changed again
            while new_root.get_id() != self.root_id:
                new_root.release_read()
                new_root = self.get_root()
                new_root.acquire_read()

            curr_node = new_root
            path = []
            # We choose one of the keys from the last node as the search key, since
            # it will be the only node on its level containing that key
            search_key = last_node.get_keys()[0]
            node_id = last_node.get_id()

            while curr_node.get_id() != node_id:
                # Add current node to path
                path.append(curr_node)
                # Find next node to add
                curr_node = curr_node.scan_node(search_key, node_id)

            # Release read lock on last_node (last_node IS curr_node at this point)
            last_node.release_read()

            path.reverse()
            return path
        finally:
            # last_node.assert_is_unlocked_by_current_thread()
            pass

    def remove_key(self, key):
        self.num_keys -= 1
        root = self.get_root()
        root.remove_key(key)

        # Root may become empty through recursive rebalancing of tree
        # If so, this means it has one child and that will become the
        # new root
        if root.is_empty():
            new_root = Node.get_node(root.children_ids[0])
            self.root_id = new_root.get_id()

    def print(self):
        curr_node = self.get_root()

        print("Printing tree...\n")
        curr_node.acquire_read()
        print(curr_node.get_keys(), curr_node.get_max_key())

        i = 0
        queue = []

        while not curr_node.is_leaf():
            children = curr_node.get_children()
            for _, child in enumerate(children):
                queue.append(child)
            curr_node.release_read()
            curr_node = queue[i]
            curr_node.acquire_read()
            i += 1
        curr_node.release_read()
        for i, node in enumerate(queue):
            node.acquire_read()
            if node.is_not_rightmost():
                print(node.get_keys(), node.get_max_key())
            else:
                print(node.get_keys(), node.get_max_key(), node.get_id())
            node.release_read()


        print("-----------------------")