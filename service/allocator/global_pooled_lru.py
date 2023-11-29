from typing import Tuple

from service.scheme import CacheScheme
from service.allocator.abstract import Allocator
from service.allocator.lru import LRULinkedList


class GlobalPooledLRU(Allocator):
    def __init__(self, scheme: CacheScheme) -> None:
        self.name = "GlobalLRU"
        self.scheme = scheme

        self.cache = LRULinkedList()

    def key_in_cache(self, tntid, key) -> bool:
        """ return whether key is in cache """
        return self.cache.key_in_cache(tntid, key)

    def cache_isfull(self) -> bool:
        """ return whether cache is full """
        return self.cache.cache_cnt >= self.scheme.cache_size

    def inform_use(self, tntid, key) -> None:
        """ called when a key is read or updated; only update last use ts """
        self.cache.inform_use(tntid, key)

    def inform_set(self, tntid, key, ttl) -> None:
        """ called when a new key is brought into cache """
        self.cache.inform_set(tntid, key, ttl)

    def arbit_evict(self, tntid, key) -> Tuple[str, str]:
        """ called when a new key should be brought in and cache is full """
        return self.cache.arbit_evict(tntid, key)
