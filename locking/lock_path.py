class LockPath:
    def __init__(self, path = []):
        self.path = path

    def release_locks(self):
        pass

    def add_read(self, node):
        self.path.append({node: node, type: 'read'})

    def add_write(self, node):
        self.path.append({node: node, type: 'write'})

    def acquire_writes(self):
        if len(path) == 2:
            parent = lock_path[0]
            print("Releasing read lock and acquiring write lock on node with keys ", parent.keys)
            parent.release_read()
            parent.acquire_write()
        elif len(lock_path) > 2:
            if lock_path[1].is_stable():
                for idx, node in enumerate(lock_path[1:-1]):
                    print("Releasing read lock and acquiring write lock on node with keys ", node.keys)
                    node.release_read()
                    node.acquire_write()
            else:
                for idx, node in enumerate(lock_path[0:-1]):
                    print("Releasing read lock and acquiring write lock on node with keys ", node.keys)
                    node.release_read()
                    node.acquire_write()
