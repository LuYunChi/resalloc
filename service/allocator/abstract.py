from service.scheme import CacheScheme
from typing import Optional


class Allocator:
    def __init__(self, scheme: CacheScheme) -> None:
        self.name = "AbstractAllocator"
        self.scheme = scheme

    def inform_get(self, tntid, key, result) -> None:
        raise NotImplementedError

    def inform_set(self, tntid, key) -> Optional[str]:
        """ return None if no need to evict, else return evict key """
        raise NotImplementedError
