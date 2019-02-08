from .base import Base
import pdb

class Traversal:
    def scan_right_for_read_guard(self, key):
        self.assert_is_read_locked_by_current_thread()
        try:
            curr_node = self
            # Want to read-lock curr_node when reading its properties
            while curr_node.is_not_rightmost() and curr_node.max_key_is_smaller_than(key):
                # print("I'm not the right node, I have keys: ", curr_node.get_keys(), "my max key is: ", curr_node.get_max_key(), "but I'm trying to add the key: ", key)
                link = curr_node.get_link()
                new_node = Base.get_node(link)
                curr_node.release_read()
                try:
                    new_node.acquire_read()
                except:
                    curr_node.acquire_read()
                    print("FAILED")
                    print("was scanning for key", key)
                    print("curr_node had keys", curr_node.get_keys(), "and max_key", curr_node.get_max_key())
                    print(curr_node.is_not_rightmost(), curr_node.max_key_is_smaller_than(key), link)
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
                try:
                    new_node.acquire_write()
                except:
                    curr_node.acquire_read()
                    print("FAILED")
                    print("was scanning for key", key)
                    print("curr_node had keys", curr_node.get_keys(),
                          "and max_key", curr_node.get_max_key())
                    print(curr_node.is_not_rightmost(),
                          curr_node.max_key_is_smaller_than(key), link)
                curr_node = new_node

            return curr_node
        finally:
            self.assert_is_unlocked_by_current_thread()
            curr_node.assert_is_write_locked_by_current_thread()

    def scan_node(self, search_key, node_id):
        self.assert_is_locked_by_current_thread()
        try:
            next_node_idx = self.find_child_idx(search_key)
            try:
                next_node_id = self.get_children_ids()[next_node_idx]
            except:
                print("Children ids: ", self.get_children_ids())
                print("Index: ", next_node_idx)
            next_node = Base.get_node(next_node_id)

            self.release_read()

            if next_node.get_id() == node_id:
                return next_node

            next_node.acquire_read()
            while next_node.is_not_rightmost() and next_node.max_key_is_smaller_than(search_key):
                prev_node = next_node
                next_node = Base.get_node(prev_node.get_link())
                prev_node.release_read()

                if next_node.get_id() == node_id:
                    return next_node

                next_node.acquire_read()

            return next_node
        finally:
            self.assert_is_unlocked_by_current_thread()

    def scan_node2(self, key, node_id):    
        print('used scan_node')
        self.assert_is_locked_by_current_thread()
        try:
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

                return child
        finally:
            self.assert_is_unlocked_by_current_thread()
            child.assert_is_read_locked_by_current_thread()
