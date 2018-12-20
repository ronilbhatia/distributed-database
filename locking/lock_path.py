class LockPath:
    def __init__(self, path = []):
        self.path = path

    def release_locks(self):
        pass

    def add(self, node):
        self.path.append(node)
