from typing import Tuple, Dict
from collections import defaultdict

from service.scheme import CacheScheme
from service.allocator.abstract import Allocator
from service.allocator.lru import LRULinkedList


class Maxmin(Allocator):
    def __init__(self, scheme: CacheScheme) -> None:
        self.name = "Max-Min"
        self.scheme = scheme

        self.cache: Dict[str, LRULinkedList] = defaultdict(LRULinkedList)

    def key_in_cache(self, tntid, key) -> bool:
        """ return whether key is in cache """
        return self.cache[tntid].key_in_cache(tntid, key)

    def cache_isfull(self) -> bool:
        """ return whether cache is full """
        cache_cnt = sum([c.cache_cnt for _, c in self.cache.items()])
        return cache_cnt >= self.scheme.cache_size

    def inform_use(self, tntid, key) -> None:
        """ called when a key is read or updated; only update last use ts """
        self.cache[tntid].inform_use(tntid, key)

    def inform_set(self, tntid, key, ttl) -> None:
        """ called when a new key is brought into cache """
        self.cache[tntid].inform_set(tntid, key, ttl)

    def arbit_evict(self, tntid, key) -> Tuple[str, str]:
        """ called when a new key should be brought in and cache is full """
        most_use_tenant = None
        most_use_cnt = 0
        assert len(self.cache)
        for tntid, cache in self.cache.items():
            if most_use_tenant is None or cache.cache_cnt > most_use_cnt:
                most_use_cnt = cache.cache_cnt
                most_use_tenant = tntid
        return self.cache[most_use_tenant].arbit_evict(tntid, key)
