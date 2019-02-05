from .base import Base
import pdb

class Traversal:
    def scan_right_for_read_guard(self, key):
        self.assert_is_read_locked_by_current_thread()
        try:
            curr_node = self
            # Want to read-lock curr_node when reading its properties
            while curr_node.is_not_rightmost() and curr_node.max_key_is_smaller_than(key):
                new_node = Base.get_node(curr_node.get_link())
                curr_node.release_read()
                new_node.acquire_read()
                curr_node = new_node

            return curr_node
        finally:
            self.assert_is_unlocked_by_current_thread()
            curr_node.assert_is_read_locked_by_current_thread()

    def scan_right_for_write_guard(self, key):
        self.assert_is_write_locked_by_current_thread()
        try:
            curr_node = self
            # Want to read-lock curr_node when reading its properties
            while curr_node.is_not_rightmost() and curr_node.max_key_is_smaller_than(key):
                new_node = Base.get_node(curr_node.get_link())
                curr_node.release_write()
                new_node.acquire_write()
                curr_node = new_node

            return curr_node
        finally:
            self.assert_is_unlocked_by_current_thread()
            curr_node.assert_is_write_locked_by_current_thread()

    def scan_node(self, key, node_id):    
        self.assert_is_locked_by_current_thread()

        if self.is_leaf():
            return self.scan_right_for_read_guard(key)
        else:
            if self.is_not_rightmost() and self.max_key_is_smaller_than(key):
                new_node = self.scan_right_for_read_guard(key)
                return new_node.scan_node(key)
            
            child_idx = self.find_child_idx(key)
            try:
                child_id = self.get_children_ids()[child_idx]
            except:
                print('children_ids: ', self.get_children_ids())
                print('key: ', key)
                print('my keys: ', self.get_keys())
            self.release_read()

            child = Base.get_node(child_id)
            # Since the node we're scanning for is already locked, we have to
            # perform this check before attempting to lock the child
            if child_id == node_id:
                return child
            child.acquire_read()

            while child.is_not_rightmost() and child.max_key_is_smaller_than(key):
                # child = child.scan_right_for_read_guard(key)
                new_node = Base.get_node(child.get_link())
                child.release_read()
                # Since the node we're scanning for is already locked, we have to 
                # perform this check before attempting to lock the new_node
                if new_node.get_id() == node_id:
                    return new_node
                    
                new_node.acquire_read()
                child = new_node

            self.assert_is_unlocked_by_current_thread()
            child.assert_is_read_locked_by_current_thread()

            return child
