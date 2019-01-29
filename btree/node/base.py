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
      if node.id in self.__nodes_map:
        raise "There is a race condition here..."
      self.__nodes_map[node.id] = node
      self.__nodes_map_lock.release_write()

    def __init__(self, keys = [], max_keys = 2, max_key = None, children_ids = []):
        self.keys = keys
        self.max_keys = max_keys
        self.min_keys = max_keys // 2
        self.max_key = max_key
        self.id = uuid.uuid4()
        self.lock = ReadWriteLock()
        self.link = None

        # Add node to hash map, ensuring it has a unique id
        while Base.get_node(self.id) is not None:
            self.id = uuid.uuid4()
        Base.add_node(self)

        self.children_ids = children_ids
