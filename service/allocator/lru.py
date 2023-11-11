from typing import Tuple

from service.scheme import CacheScheme
from service.allocator.abstract import Allocator


class CacheObject:
    def __init__(self, tntid, key) -> None:
        self.tntid = tntid
        self.key = key

        self.pre: CacheObject = None
        self.next: CacheObject = None


class GlobalLRU(Allocator):
    def __init__(self, scheme: CacheScheme) -> None:
        self.name = "GlobalLRU"
        self.scheme = scheme

        self.cache_cnt = 0
        self.head = CacheObject(None, None)
        self.tail = CacheObject(None, None)

    def _get_ptr(self, tntid, key) -> CacheObject:
        ptr: CacheObject = self.head
        while ptr is not None:
            if ptr.tntid == tntid and ptr.key == key:
                return ptr
            ptr = ptr.next
        return None

    def key_in_cache(self, tntid, key) -> bool:
        """ return whether key is in cache """
        return self._get_ptr(tntid, key) is not None

    def cache_isfull(self) -> bool:
        """ return whether cache is full """
        return self.cache_cnt >= self.scheme.cache_size

    def _insert_front(self, co: CacheObject) -> None:
        co.pre = self.head
        co.next = self.head.next
        co.pre.next = co
        co.next.pre = co

    def inform_use(self, tntid, key) -> None:
        """ called when a key is read or updated; only update last use ts """
        ptr = self._get_ptr(tntid, key)
        assert ptr is not None
        ptr.pre.next = ptr.next
        ptr.next.pre = ptr.pre
        self._insert_front(ptr)

    def inform_set(self, tntid, key) -> None:
        """ called when a new key is brought into cache """
        assert self.cache_cnt < self.cache_size
        self.cache_cnt += 1
        co = CacheObject(tntid, key)
        self._insert_front(co)

    def arbit_evict(self, tntid, key) -> Tuple[str, str]:
        """ called when a new key should be brought in and cache is full """
        assert self.cache_cnt == self.cache_size
        self.cache_cnt -= 1
        last = self.tail.pre
        last.pre.next = last.next
        last.next.pre = last.pre
        evict_tntid = last.tntid
        evict_key = last.key
        del last
        return evict_tntid, evict_key
