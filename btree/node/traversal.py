class Traversal:
  def scan_right_for_write_guard(self, key):
    print("Scanning...")
    curr_node = Node.get_node(self.get_link())

    # Want to read-lock curr_node when reading its properties
    while curr_node.is_not_rightmost() and key > curr_node.get_max_key():
        curr_node = Node.get_node(self.get_link())

    return curr_node

    def scan_node(self, key):
      self.assert_is_locked_by_current_thread()
      child_idx = self.find_idx(key)

      if self.is_not_rightmost() and key > self.get_max_key():
          new_node = self.scan_right_for_write_guard(key)

          # Release read on old node and acquire read on new node
          self.release_read()
          new_node.acquire_read()

          return new_node.scan_node(key)

      return self.get_children_ids()[child_idx]
