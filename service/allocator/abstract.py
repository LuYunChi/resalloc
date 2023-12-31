from typing import Tuple

from service.scheme import CacheScheme


class Allocator:
    def __init__(self, scheme: CacheScheme) -> None:
        self.name = "AbstractAllocator"
        self.scheme = scheme

    @property
    def num_tnts(self) -> int:
        return self.scheme.num_tenants

    @property
    def cache_size(self) -> int:
        return self.scheme.cache_size

    def key_in_cache(self, tntid, key) -> bool:
        """ return whether key is in cache """
        raise NotImplementedError("key_in_cache")

    def cache_isfull(self) -> bool:
        """ return whether cache is full """
        raise NotImplementedError("cache_isfull")

    def inform_use(self, tntid, key) -> None:
        """ called when a key is read or updated; only update last use ts """
        raise NotImplementedError("inform_use")

    def inform_set(self, tntid, key, ttl) -> None:
        """ called when a new key is brought into cache """
        raise NotImplementedError("inform_set")

    def arbit_evict(self, tntid, key) -> Tuple[str, str]:
        """ called when a new key should be brought in and cache is full """
        raise NotImplementedError("arbit_evict")
