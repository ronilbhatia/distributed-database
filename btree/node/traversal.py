class Traversal:
  def scan_right_for_write_guard(self, key):
    print("Scanning...")
    curr_node = Node.get_node(self.link)

    # Want to read-lock curr_node when reading its properties
    while curr_node.is_not_rightmost() and key > curr_node.max_key:
        curr_node = Node.get_node(self.link)

    return curr_node

    def scan_node(self, key):
      if not self.locked():
          raise 'Node is not locked'
      child_idx = self.find_idx(key)

      if self.is_not_rightmost() and key > self.max_key:
          new_node = self.scan_right_for_write_guard(key)

          # Release read on old node and acquire read on new node
          self.release_read()
          new_node.acquire_read()

          return new_node.scan_node(key)

      return self.children_ids[child_idx]
