
import hashlib
import bisect

class ConsistentHashing:
    def __init__(self, num_replicas=3):
        self.replicas = num_replicas
        self.ring = {}
        self.sorted_keys = []
        self.nodes = set()

    def _hash(self, key):
        """Hash function to get a numerical value for a key"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % (2**32)

    def add_server(self, server):
        """Add a server to the ring with replicas"""
        self.nodes.add(server)
        for i in range(self.replicas):
            key = self._hash(f"{server}-{i}")
            self.ring[key] = server
            bisect.insort(self.sorted_keys, key)

    def remove_server(self, server):
        """Remove a server from the ring"""
        self.nodes.remove(server)
        for i in range(self.replicas):
            key = self._hash(f"{server}-{i}")
            self.ring.pop(key, None)
            self.sorted_keys.remove(key)

    def get_server(self, key):
        """Find the closest server in the clockwise direction"""
        if not self.ring:
            return None

        key_hash = self._hash(key)
        index = bisect.bisect(self.sorted_keys, key_hash) % len(self.sorted_keys)
        return self.ring[self.sorted_keys[index]]

# Usage Example
ch = ConsistentHashing()
ch.add_server("S1")
ch.add_server("S2")
ch.add_server("S3")

print(ch.get_server("Key_A"))  # Returns the server responsible for Key_A
print(ch.get_server("Key_B"))

ch.add_server("S4")  # Adding a new server
print(ch.get_server("Key_A"))  # Some keys might be reassigned
