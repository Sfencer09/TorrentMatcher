import os
from typing import Dict, List


class DownloadedFile:
    path: str
    size: int
    # hash_cache[piece_size][offset] = pieces
    hash_cache: Dict[int, Dict[int, bytes]]
    
    def __init__(self, path: str, size: int, hash_cache: Dict[int, Dict[int, bytes]]={}):
        self.size = size
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
    
    # def calculate_hashes(self, piece_length: int, offsets: List[int]) -> Dict[int, bytes]: