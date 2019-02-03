class Locking:
  # == LOCKING METHODS ==
  #
  # We will rely on the ReadWriteLock methods to protect us from
  # reacquisition.
  def acquire_read(self):
    self.get_lock().acquire_read()

  def acquire_write(self):
    self.get_lock().acquire_write()

  def release_read(self):
    self.get_lock().release_read()

  def release_write(self):
    self.get_lock().release_write()

  def assert_is_locked_by_current_thread(self):
    self.get_lock().assert_is_locked_by_current_thread()

  def assert_is_read_locked_by_current_thread(self):
    self.get_lock().assert_is_read_locked_by_current_thread()

  def assert_is_unlocked_by_current_thread(self):
    self.get_lock().assert_is_unlocked_by_current_thread()

  def assert_is_write_locked_by_current_thread(self):
    self.get_lock().assert_is_write_locked_by_current_thread()
