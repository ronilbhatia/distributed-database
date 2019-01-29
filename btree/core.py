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
        self.root_id = initial_root.id

    def build_new_root(self, old_root, split_info):
        keys = [split_info['median']]
        max_keys = old_root.max_keys
        max_key = Node.get_node(split_info['right_id']).max_key
        children_ids = [split_info['left_id'], split_info['right_id']]

        # Create new root
        new_root = Node(keys, max_keys, max_key, children_ids)
        self.root_id = new_root.id
        
        # Release lock on old root
        old_root.release_write()
        self.print()

    def add_key(self, key):
        print("Adding key ", key)
        self.num_keys += 1

        root = self.get_root()
        split_info = root.add_key(key)

        if split_info:
            root.acquire_write()

            if self.root_id == root.id:
                self.build_new_root(root, split_info)
            else:
                # The root is no longer the root. We must re-descend to the node 
                # we last split from the new root, and potentially by the time
                # we finish doing that the new root will again no longer be the root
                # Thus, this process is a loop.
                while self.root_id != root.id:
                    root.release_write()
                    # Since the root was split, it's possible that it isn't even
                    # the node we want to continue propagating up from
                    while root.is_not_rightmost() and key > root.max_key:
                        new_root = root.scan_right_for_write_guard(key)
                        root = new_root

                    # Redescend from new root node to find path
                    path = self.find_path_from_new_root(root)
                    root = path.pop()

                    for node in path:
                        node.acquire_write()

                        # Again, make sure this is the correct node to handle split
                        while node.is_not_rightmost() and key > node.max_key:
                            node.release_write()
                            new_node = node.scan_right_for_write_guard(key)
                            new_node.acquire_write()
                            node = new_node
                        
                        child_idx = node.find_idx(key)

                        # handle_split will take care of releasing write lock
                        split_info = node.handle_split(split_info, child_idx) 

                        # If there is no split_info, there is no further propagation required
                        if not split_info:
                            return
                    
                    root.acquire_write()
                    if self.root_id == root.id:
                        return self.build_new_root(root, split_info)

    def find_path_from_new_root(self, old_root):
        new_root = self.get_root()
        new_root.acquire_read()

        # By the time we acquire read, root might have changed again
        while new_root.id != self.root_id:
            new_root.release_read()
            new_root = self.get_root()
            new_root.acquire_read()
            
        curr_node = new_root
        path = []
        search_key = old_root.keys[0]

        while curr_node.id != old_root.id:
            # Add current node to path
            path.append(curr_node)
            
            # Find next node to add and release read lock
            next_node_id = curr_node.scan_node(search_key)
            curr_node.release_read()

            curr_node = Node.get_node(next_node_id)
            curr_node.acquire_read()

        # Release read lock on old root (old root IS curr_node at this point)
        curr_node.release_read()

        path.reverse()
        return path

    def remove_key(self, key):
        self.num_keys -= 1
        root = self.get_root()
        root.remove_key(key)

        # Root may become empty through recursive rebalancing of tree
        # If so, this means it has one child and that will become the
        # new root
        if root.is_empty():
            new_root = Node.get_node(root.children_ids[0])
            self.root_id = new_root.id

    def print(self):
        curr_node = self.get_root()

        print("Printing tree...\n")
        print(curr_node.keys, curr_node.max_key)

        i = 0
        queue = []

        while not curr_node.is_leaf():
            children = curr_node.get_children()
            for _, child in enumerate(children):
                queue.append(child)

            curr_node = queue[i]
            i += 1

        for i, node in enumerate(queue):
            if node.is_not_rightmost():
                print(node.keys, node.max_key, node.link)
            else:
                print(node.keys, node.max_key, node.id)


        print("-----------------------")


### build tree
# btree = BTree(4)
# root = Node([13, 24, 30], 4)
# child_one = Node([2, 3, 5, 7], 4, 14)
# child_two = Node([14, 16, 19, 22], 4, 24)
# child_three = Node([24, 27, 29], 4, 33)
# child_four = Node([33, 34, 38, 39], 4)
# root.children_ids = [child_one.id, child_two.id, child_three.id, child_four.id]
# btree.root_id = root.id
# child_one.link = child_two.id
# child_two.link = child_three.id
# child_three.link = child_four.id

# btree.print()
# ### Test adding keys
# # Add key to leaf
# btree.add_key(28)
# # Add key causing leaf split, while parent(root) has space
# btree.add_key(20)
# btree.print()

# # Add key causing leaf split, and parent(root) split -> increase depth of tree
# btree.add_key(8)

# # Add some more keys
# btree.add_key(10)
# btree.add_key(37)

# btree.print()

# ### Test removing keys
# # Remove key from leaf
# # btree.remove_key(39)
# # btree.remove_key(28)

# # # Remove key from leaf, causing rotation right (from left sibling)
# # btree.remove_key(33)

# # # Remove key from leaf, causing rotation left (from right sibling)
# # btree.remove_key(3)

# # # Remove key from leaf, causing merge left (from right sibling)
# # btree.remove_key(37)

# # # Setup for next test
# # btree.add_key(37)
# # btree.add_key(39)
# # btree.remove_key(24)

# # # Remove key from leaf, causing merge right (from left sibling)
# # btree.remove_key(27)

# # # Remove additional key to setup next test
# # btree.remove_key(16)

# # # Remove key causing rebalance propagating to the root, emptying it, and
# # # creating a new root
# # btree.remove_key(8)

# btree.print()
# # pdb.set_trace()
# print("done")

# # If the root is a leaf and it overflowed handle here
#         if split_info == 'leaf overflow':
#             print(root.lock.locked())
#             root_split_info = root.split()

#             if self.root_id == root.id:
#                 self.build_new_root(root, root_split_info)
#                 root.release_write()
#                 print("Created new root and unlocked old root")
#             else:
#                 # Build path from new root to old root
#                 new_root = self.get_root()
#                 curr_node = new_root
#                 path = []
#                 search_key = root.keys[0]

#                 while curr_node.id != root.id:
#                     path.append(curr_node)
#                     next_node_id = curr_node.scan_node(search_key)
#                     curr_node = Node.get_node(next_node_id)

#                 path.reverse()
#                 split_info = root_split_info
#                 print(root.lock.locked(), root.keys)
#                 self.print()
#                 root.release_write()

#                 # Handle split going up from old root, while ensuring we are at
#                 # the appropriate node on each level (could have changed since
#                 # building path)
#                 for _, node in enumerate(path):
#                     if split_info:
#                         split_info['split_node'].release_write()

#                     node.acquire_write()
#                     while node.is_not_rightmost() and key > node.max_key:
#                         node.release_write()
#                         new_node = node.scan_right_for_write_guard(key)
#                         new_node.acquire_write()
#                         node = new_node

#                     if split_info:
#                         split_info = node.handle_split(split_info, child_idx)
#                     else:
#                         return
#         # Otherwise if there is split info and the root is not a leaf
#         elif split_info:
#             root.acquire_write()
#             print('Locked non-leaf root with keys ', root.keys)

#             # If this is still the root, then we must create a new root
#             if self.root_id == root.id:
#                 root_split_info = root.handle_split(split_info, child_idx)

#                 if root_split_info:
#                     self.build_new_root(root, root_split_info)
#                     root.release_write()
#             # Otherwise, trace path from new root to here and continue splitting
#             # upwards
#             else:
#                 new_root = self.get_root()
#                 curr_node = new_root
#                 path = []
#                 search_key = root.keys[0]

#                 while curr_node.id != root.id:
#                     path.append(curr_node)
#                     next_node_id = curr_node.scan_node(search_key)
#                     curr_node = Node.get_node(next_node_id)

#                 print("Made it back to the root from internal! Now what...")
#                 path.reverse()

#                 # Handle split going up from old root, while ensuring we are at
#                 # the appropriate node on each level (could have changed since
#                 # building path)
#                 for _, node in enumerate(path):
#                     if split_info:
#                         split_info['split_node'].release_write()

#                     node.acquire_write()
#                     while node.is_not_rightmost() and key > node.max_key:
#                         node.release_write()
#                         new_node = node.scan_right_for_write_guard(key)
#                         new_node.acquire_write()
#                         node = new_node

#                     if split_info:
#                         split_info = node.handle_split(split_info, child_idx)
#                     else:
#                         return

            # if self.is_not_rightmost() and key > self.max_key:
            #     self.release_write()
            #     new_node = self.scan_right_for_write_guard(key)
            #     new_node.acquire_write()
            #     return new_node.handle_split(split_info, child_idx)

            # return res

    # def add_key2(self, key):
    #     print("Adding key ", key)
    #     self.num_keys += 1

    #     root = self.get_root()
    #     current = root
    #     path = []

    #     while not current.is_leaf():
    #         path.append(current)
    #         next = current.scan_node(key)
    #         current = Node.get_node(next)

    #     current.add_key_leaf()
