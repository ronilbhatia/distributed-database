import pdb

class Deletion:
    def remove_key(self, key):
        # Check if node is leaf - attempt to remove key directly
        if self.is_leaf():
            return self.remove_key_leaf(key)
        else:
            return self.remove_key_internal(key)

    def remove_key_leaf(self, key):
        try:
            self.keys.remove(key)
        except:
            print("key does not exist")
            return

        # Check it underflow has occurred and need to rebalance
        if self.is_deficient():
            return True

    def remove_key_internal(self, key):
        children = self.get_children()
        child_idx = self.find_idx(key)

        res = children[child_idx].remove_key(key)

        # having a res implies underflow occurred when removing key
        # from child and we must restructure the tree
        if res:
            return self.rebalance(child_idx)

    def rebalance(self, child_idx):
        child = self.get_child_at_index(child_idx)
        left_sibling = self.get_child_at_index(child_idx - 1)
        right_sibling = self.get_child_at_index(child_idx + 1)

        # Try to rotate from right sibling first, if it can give up keys
        if right_sibling is not None and right_sibling.can_give_up_keys():
            return self.rotate_left(child, child_idx, right_sibling)

        # Otherwise, try  to rotate from left sibling
        elif left_sibling is not None and left_sibling.can_give_up_keys():
            return self.rotate_right(child, child_idx, left_sibling)

        # Otherwise, must merge siblings
        else:
            # if it's last child then merge with left sibling
            if child_idx == self.num_keys():
                self.merge_left(child, left_sibling)

            # otherwise merge with right sibling
            else:
                self.merge_right(child, child_idx, right_sibling)

            # Internal node may now be under because of merge
            # operation - may need to rebalance
            if self.is_deficient():
                return True

    def rotate_left(self, child, child_idx, right_sibling):
        rotate_key = right_sibling.keys[0]
        new_separator = right_sibling.keys[1]

        right_sibling.keys.remove(rotate_key)
        self.keys[child_idx] = new_separator
        if not right_sibling.is_leaf():
            new_child_id = right_sibling.children_ids[0]
            self.children_ids.append(new_child_id)
            del(right_sibling.children_ids[0])

        self.lock.acquire_read()
        child.lock.acquire_read()
        lock_path = [self, child]
        # pdb.set_trace()
        child.add_key(rotate_key, lock_path)

    def rotate_right(self, child, child_idx, left_sibling):
        rotate_key = left_sibling.keys[-1]
        self.keys[child_idx-1] = rotate_key

        left_sibling.keys.remove(rotate_key)
        if not left_sibling.is_leaf():
            new_child_id = left_sibling.children_ids[-1]
            self.children_ids.insert(0, new_child_id)
            del(right_sibling.children_ids[-1])

        self.lock.acquire_read()
        child.lock.acquire_read()
        lock_path = [self, child]
        # pdb.set_trace()
        child.add_key(rotate_key, lock_path)

    def merge_left(self, child, left_sibling):
        # TODO: Change method to be dynamic and not rely on node being
        # last child. Could even leverage merge_right method instead.
        separator = self.keys[-1]

        self.keys.pop()
        if not child.is_leaf():
            left_sibling.keys.append(separator)

        self.children_ids.pop()

        left_sibling.keys = left_sibling.keys + child.keys
        left_sibling.children_ids = left_sibling.children_ids + child.children_ids

    def merge_right(self, child, child_idx, right_sibling):
        separator = self.keys[child_idx]

        self.keys.remove(separator)
        if not child.is_leaf():
            right_sibling.keys.insert(0, separator)

        self.children_ids.remove(child.id)

        right_sibling.keys = child.keys + right_sibling.keys
        right_sibling.children_ids = child.children_ids + right_sibling.children_ids
