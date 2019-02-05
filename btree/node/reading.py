from .base import Base

class Reading:
  # == READ METHODS ==
  #
  # These will all require a lock for reading. But they don't do any
  # writing.

  def get_child_at_index(self, child_idx):
    self.assert_is_locked_by_current_thread()
    try:
      if child_idx < 0 or child_idx >= len(self.get_children_ids()):
          return None
      else:
          return Base.get_node(self.get_children_ids()[child_idx])
    finally:
      self.assert_is_locked_by_current_thread()

  # gives a list comprehension, taking each child id and looking up in
  # hash map
  def get_children(self):
    self.assert_is_locked_by_current_thread()
    try:
      children = [Base.get_node(id) for id in self.get_children_ids()]
      return children
    finally:
      self.assert_is_locked_by_current_thread()

  def num_keys(self):
    self.assert_is_locked_by_current_thread()
    try:
      return len(self.get_keys())
    finally:
      self.assert_is_locked_by_current_thread()

  def is_full(self):
    self.assert_is_locked_by_current_thread()
    try:
      return self.num_keys() == self.get_max_keys()
    finally:
      self.assert_is_locked_by_current_thread()

  def is_stable(self):
    self.assert_is_locked_by_current_thread()
    try:
      return self.num_keys() < self.get_max_keys()
    finally:
      self.assert_is_locked_by_current_thread()

  def overflow(self):
    self.assert_is_locked_by_current_thread()
    try:
      return self.num_keys() > self.get_max_keys()
    finally:
      self.assert_is_locked_by_current_thread()

  def is_empty(self):
    self.assert_is_locked_by_current_thread()
    try:
      return self.num_keys() == 0
    finally:
      self.assert_is_locked_by_current_thread()

  def is_leaf(self):
    self.assert_is_locked_by_current_thread()
    try:
      return len(self.get_children_ids()) == 0
    finally:
      self.assert_is_locked_by_current_thread()

  def can_give_up_keys(self):
    self.assert_is_locked_by_current_thread()
    try:
      return self.get_min_keys() < self.num_keys()
    finally:
      self.assert_is_locked_by_current_thread()

  def is_deficient(self):
    self.assert_is_locked_by_current_thread()
    try:
      return self.num_keys() < self.get_min_keys()
    finally:
      self.assert_is_locked_by_current_thread()

  def is_last_child(self, child_id):
    self.assert_is_locked_by_current_thread()
    try:
      return child_id == self.get_children_ids()[-1]
    finally:
      self.assert_is_locked_by_current_thread()

  def is_not_rightmost(self):
    self.assert_is_locked_by_current_thread()
    try:
      return self.get_max_key() is not None
    finally:
      self.assert_is_locked_by_current_thread()

  def find_child_idx(self, key):
    self.assert_is_locked_by_current_thread()
    try:
      found_child_idx = False
      idx = None

      for i, curr_key in enumerate(self.get_keys()):
          if key < curr_key:
              found_child_idx = True
              idx = i
              break

      if not found_child_idx:
          idx = self.num_keys()

      return idx
    finally:
      self.assert_is_locked_by_current_thread()

  def max_key_is_smaller_than(self, key):
    self.assert_is_locked_by_current_thread()
    try:
      return self.get_max_key() <= key
    finally:
      self.assert_is_locked_by_current_thread()