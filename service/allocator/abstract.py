from typing import Tuple
from service.scheme import CacheScheme


class Allocator:
    def __init__(self, scheme: CacheScheme) -> None:
        self.name = "AbstractAllocator"
        self.scheme = scheme

    def key_in_cache(self, tntid, key) -> bool:
        """ return whether key is in cache """
        pass

    def cache_isfull(self) -> bool:
        """ return whether cache is full """
        pass

    def inform_use(self, tntid, key) -> None:
        """ called when a key is read or updated; only update last use ts """
        raise NotImplementedError

    def inform_set(self, tntid, key) -> None:
        """ called when a new key is brought into cache """
        raise NotImplementedError

    def arbit_evict(self, tntid, key) -> Tuple[str, str]:
        """ called when a new key should be brought in and cache is full """
        raise NotImplementedError
