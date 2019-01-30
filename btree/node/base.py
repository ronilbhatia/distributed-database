from locking.lock import ReadWriteLock
import uuid

class Base:
    # Class variable to store all nodes
    __nodes_map = {}
    __nodes_map_lock = ReadWriteLock()

    @classmethod
    def get_node(self, id):
      self.__nodes_map_lock.acquire_read()
      node = self.__nodes_map.get(id)
      self.__nodes_map_lock.release_read()
      return node

    @classmethod
    def add_node(self, node):
      self.__nodes_map_lock.acquire_write()
      if node.get_id() in self.__nodes_map:
        raise "There is a race condition here..."
      self.__nodes_map[node.get_id()] = node
      self.__nodes_map_lock.release_write()

    def __init__(self, keys = [], max_keys = 2, max_key = None, children_ids = []):
        self.__keys = keys
        self.__max_keys = max_keys
        self.__min_keys = max_keys // 2
        self.__max_key = max_key
        self.__id = uuid.uuid4()
        self.__lock = ReadWriteLock()
        self.__link = None

        # Add node to hash map, ensuring it has a unique id
        while Base.get_node(self.get_id()) is not None:
            self.__id = uuid.uuid4()
        Base.add_node(self)

        self.__children_ids = children_ids

    def get_id(self):
      # This method is safe because the __id property is immutable.
      return self.__id

    def get_lock(self):
      # This method is safe because the __lock property is immutable.
      return self.__lock

    def get_children_ids(self):
      self.assert_is_locked_by_current_thread()
      # NR: Should return a tuple to protect immutability.
      return self.__children_ids

    def set_children_ids(self, new_children_ids):
      self.assert_is_write_locked_by_current_thread()
      self.__children_ids = new_children_ids

    def get_keys(self):
      self.assert_is_locked_by_current_thread()
      # NR: Should return a tuple to protect immutability.
      return self.__keys

    def set_keys(self, new_keys):
      self.assert_is_write_locked_by_current_thread()
      self.__keys = new_keys

    def get_link(self):
      self.assert_is_locked_by_current_thread()
      return self.__link

    def set_link(self, new_link):
      self.assert_is_write_locked_by_current_thread()
      self.__link = new_link

    def get_max_key(self):
      self.assert_is_locked_by_current_thread()
      return self.__max_key

    def set_max_key(self, new_max_key):
      self.assert_is_write_locked_by_current_thread()
      self.__max_key = new_max_key

    def get_max_keys(self):
      self.assert_is_locked_by_current_thread()
      return self.__max_keys

    def get_min_keys(self):
      self.assert_is_locked_by_current_thread()
      return self.__min_keys
