import threading
from threading import Thread, Lock

class ReadWriteLock(object):
    """ An implementation of a read-write lock for Python.
    Any number of readers can work simultaneously but they
    are mutually exclusive with any writers (which can
    only have one at a time).

    This implementation is reader biased. This can be harmful
    under heavy load because it can starve writers.
    However under light load it will be quite perfomant since
    reads are usually much less resource intensive than writes,
    and because it maximizes concurrency.
    """

    def __init__(self):
        self.__monitor = Lock()
        self.__exclude = Lock()

        self.__id_lock = Lock()
        self.__reader_ids = []
        self.__writer_id = None

    def acquire_read(self):
      thread_id = threading.get_ident()
      with self.__id_lock:
        if thread_id in self.__reader_ids:
          raise Exception("Thread is trying to reacquire read lock?")
        if thread_id == self.__writer_id:
          raise Exception("Thread is trying to acquire read lock when has write lock?")

      with self.__monitor:
        num_readers = None
        with self.__id_lock:
          self.__reader_ids.append(thread_id)
          num_readers = len(self.__reader_ids)
        if num_readers == 1:
          self.__exclude.acquire()

    def release_read(self):
      thread_id = threading.get_ident()
      with self.__id_lock:
        if thread_id not in self.__reader_ids:
          raise Exception("Thread is trying to release read lock it doesn't own?")

      with self.__monitor:
        num_readers = None
        with self.__id_lock:
          self.__reader_ids.remove(thread_id)
          num_readers = len(self.__reader_ids)

        if num_readers == 0:
          self.__exclude.release()

    def acquire_write(self):
      with self.__id_lock:
        thread_id = threading.get_ident()
        if thread_id in self.__reader_ids:
          raise Exception("Trying to acquire write lock when we already have read lock?")
        if thread_id == self.__writer_id:
          raise Exception("Trying to reacquire write lock second time?")

      self.__exclude.acquire()
      with self.__id_lock:
        if self.__writer_id is not None:
          raise Exception("Was __writer_id not cleared?")
        self.__writer_id = threading.get_ident()

    def release_write(self):
      with self.__id_lock:
        thread_id = threading.get_ident()
        if thread_id != self.__writer_id:
          raise Exception("Thread is trying to release a write lock we don't own?")
        self.__writer_id = None
      self.__exclude.release()

    def assert_is_locked_by_current_thread(self):
      with self.__id_lock:
        if threading.get_ident() in self.__reader_ids:
          return
        if threading.get_ident() == self.__writer_id:
          return
        raise Exception("Expected thread to hold this lock.")

    def assert_is_read_locked_by_current_thread(self):
      with self.__id_lock:
        if threading.get_ident() not in self.__reader_ids:
          raise Exception("Expected to be read locked by current thread")

    def assert_is_write_locked_by_current_thread(self):
      with self.__id_lock:
        if threading.get_ident() != self.__writer_id:
          raise Exception("Expected to be write locked by current thread")

    def assert_is_unlocked_by_current_thread(self):
      with self.__id_lock:
        if threading.get_ident() in self.__reader_ids:
          raise Exception("Didn't expect to be read locked by current thread.")
        if threading.get_ident() == self.__writer_id:
          raise Exception("Didn't expect to be write locked by current thread.")
