import os
from typing import Dict


class DownloadedFile:
    path: str
    size: int
    # hash_cache[piece_size][offset] = pieces
    hash_cache: Dict[int, Dict[int, bytes]]
    
    def __init__(self, path: str, hash_cache: Dict[int, Dict[int, bytes]]={}):
        self.size = os.path.getsize(path)
        self.path = path
        self.hash_cache = hash_cache
    
    def add_hash(self, piece_size: int, offset: int, hash: bytes):
        if not self.hash_cache[piece_size]:
            self.hash_cache[piece_size] = {offset: hash}
        else:
            self.hash_cache[piece_size][offset] = hash
    
    def get_hash(self, piece_size: int, offset: int):
        if not self.hash_cache[piece_size]:
            return None
        return self.hash_cache[piece_size][offset]
    
    