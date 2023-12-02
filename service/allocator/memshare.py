import random
from typing import Tuple, Dict, List
from collections import defaultdict

from service.scheme import CacheScheme
from service.allocator.abstract import Allocator
from service.allocator.lru import LRULinkedList


class Memshare(Allocator):
    def __init__(self, scheme: CacheScheme) -> None:
        self.name = "Memshare"
        self.scheme = scheme

        self.victim_qs: Dict[str, List[str]] = defaultdict(list)
        self.cache: Dict[str, LRULinkedList] = defaultdict(LRULinkedList)

    @property
    def guar_size(self):
        return max(1, int(self.scheme.cache_size
                   / self.scheme.num_tenants
                   * self.scheme.guarantee_ratio))

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

    def _append_vq(self, tntid, key) -> None:
        self.victim_qs[tntid].append(key)
        # victim q has same len as guaranteed
        if len(self.victim_qs[tntid]) > self.guar_size:
            self.victim_qs[tntid] = self.victim_qs[tntid][1:]

    def arbit_evict(self, tntid, key) -> Tuple[str, str]:
        """ called when a new key should be brought in and cache is full """
        victen_cands = []
        for t, c in self.cache.items():
            if c.cache_cnt > self.guar_size and t != tntid:
                lut = c.oldest_time()
                assert lut is not None
                victen_cands.append((lut, t))

        if len(victen_cands) == 0 or (key not in self.victim_qs[tntid] and self.cache[tntid].cache_cnt > self.guar_size):
            etntid, ekey = self.cache[tntid].arbit_evict(tntid, key)
            self._append_vq(etntid, ekey)
            return etntid, ekey

        _, victim = sorted(victen_cands)[0]
        etntid, ekey = self.cache[victim].arbit_evict(tntid, key)
        self._append_vq(etntid, ekey)
        return etntid, ekey
