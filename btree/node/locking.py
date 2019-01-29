class Locking:
  # == LOCKING METHODS ==
  #
  # We will rely on the ReadWriteLock methods to protect us from
  # reacquisition.
  def acquire_read(self):
      self.lock.acquire_read()

  def acquire_write(self):
      self.lock.acquire_write()

  def release_read(self):
      self.lock.release_read()

  def release_write(self):
      self.lock.release_write()
