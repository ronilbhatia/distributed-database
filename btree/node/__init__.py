from .base import Base
# from .deletion import Deletion
from .insertion import Insertion
from .locking import Locking
from .reading import Reading
from .traversal import Traversal

class Node(Base, Insertion, Locking, Reading, Traversal):
  pass
