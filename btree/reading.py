class Reading:
  # == READ METHODS ==
  #
  # These will all require a lock for reading. But they don't do any
  # writing.

  def get_child_at_index(self, child_idx):
    self.lock.assert_is_locked_by_current_thread()
    try:
      if child_idx < 0 or child_idx >= len(self.children_ids):
          return None
      else:
          return Node.get_node(self.children_ids[child_idx])
    finally:
      self.lock.assert_is_locked_by_current_thread()

  # gives a list comprehension, taking each child id and looking up in
  # hash map
  def get_children(self):
    self.lock.assert_is_locked_by_current_thread()
    try:
      # children = map(lambda id: Node.get_node(id), self.children_ids)
      children = [Node.get_node(id) for id in self.children_ids]
      return children
    finally:
      self.lock.assert_is_locked_by_current_thread()

  def num_keys(self):
    self.lock.assert_is_locked_by_current_thread()
    try:
      return len(self.keys)
    finally:
      self.lock.assert_is_locked_by_current_thread()

  def is_full(self):
    self.lock.assert_is_locked_by_current_thread()
    try:
      return self.num_keys() == self.max_keys
    finally:
      self.lock.assert_is_locked_by_current_thread()

  def is_stable(self):
    self.lock.assert_is_locked_by_current_thread()
    try:
      return self.num_keys() < self.max_keys
    finally:
      self.lock.assert_is_locked_by_current_thread()

  def overflow(self):
    self.lock.assert_is_locked_by_current_thread()
    try:
      return self.num_keys() > self.max_keys
    finally:
      self.lock.assert_is_locked_by_current_thread()

  def is_empty(self):
    self.lock.assert_is_locked_by_current_thread()
    try:
      return self.num_keys() == 0
    finally:
      self.lock.assert_is_locked_by_current_thread()

  def is_leaf(self):
    self.lock.assert_is_locked_by_current_thread()
    try:
      return len(self.children_ids) == 0
    finally:
      self.lock.assert_is_locked_by_current_thread()

  def can_give_up_keys(self):
    self.lock.assert_is_locked_by_current_thread()
    try:
      return self.min_keys < self.num_keys()
    finally:
      self.lock.assert_is_locked_by_current_thread()

  def is_deficient(self):
    self.lock.assert_is_locked_by_current_thread()
    try:
      return self.num_keys() < self.min_keys
    finally:
      self.lock.assert_is_locked_by_current_thread()

  def is_last_child(self, child_id):
    self.lock.assert_is_locked_by_current_thread()
    try:
      return child_id == self.children_ids[-1]
    finally:
      self.lock.assert_is_locked_by_current_thread()

  def is_not_rightmost(self):
    self.lock.assert_is_locked_by_current_thread()
    try:
      return self.max_key is not None
    finally:
      self.lock.assert_is_locked_by_current_thread()

  def find_idx(self, key):
    self.lock.assert_is_locked_by_current_thread()
    try:
      found_idx = False
      idx = None

      for i, curr_key in enumerate(self.keys):
          if key < curr_key:
              found_idx = True
              idx = i
              break

      if not found_idx:
          idx = self.num_keys()

      return idx
    finally:
      self.lock.assert_is_locked_by_current_thread()
