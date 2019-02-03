from .base import Base

class Traversal:
    def scan_right_for_read_guard(self, key):
        self.assert_is_locked_by_current_thread()
        curr_node = self

        # Want to read-lock curr_node when reading its properties
        while curr_node.is_not_rightmost() and curr_node.max_key_is_smaller_than(key):
            new_node = Base.get_node(curr_node.get_link())
            curr_node.release_read()
            new_node.acquire_read()
            curr_node = new_node

        self.assert_is_unlocked_by_current_thread()
        curr_node.assert_is_read_locked_by_current_thread()
        return curr_node

    def scan_right_for_write_guard(self, key):
        self.assert_is_write_locked_by_current_thread()
        curr_node = self

        # Want to read-lock curr_node when reading its properties
        while curr_node.is_not_rightmost() and curr_node.max_key_is_smaller_than(key):
            new_node = Base.get_node(curr_node.get_link())
            curr_node.release_write()
            new_node.acquire_write()
            curr_node = new_node

        self.assert_is_unlocked_by_current_thread()
        curr_node.assert_is_write_locked_by_current_thread()
        return curr_node

    def scan_node(self, key):
        self.assert_is_locked_by_current_thread()
        child_idx = self.find_idx(key)

        if self.is_not_rightmost() and self.max_key_is_smaller_than(key):
            new_node = self.scan_right_for_read_guard(key)
            # Release read on old node and acquire read on new node
            self.release_read()
            new_node.acquire_read()

            return new_node.scan_node(key)
        
        child_idx = self.get_children_ids()[child_idx]
        self.release_read()

        child = Base.get_node(child_idx)
        child.acquire_read()

        if child.is_not_rightmost() and child.max_key_is_smaller_than(key):
            new_node = child.scan_right_for_read_guard(key)
            # Release read on old node and acquire read on new node
            child.release_read()
            new_node.acquire_read()

        self.assert_is_unlocked_by_current_thread()
        child.assert_is_read_locked_by_current_thread()

        return child
