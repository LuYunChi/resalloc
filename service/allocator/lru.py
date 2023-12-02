import time
from typing import Optional, Tuple

from service.scheme import CacheScheme
from service.allocator.abstract import Allocator


class CacheObject:
    def __init__(self, tntid, key, create_time, ttl) -> None:
        self.tntid = tntid
        self.key = key
        self.last_used = create_time
        self.ttl = ttl

        self.pre: CacheObject = None
        self.next: CacheObject = None


class LRULinkedList:
    def __init__(self) -> None:
        self.name = "LRULinkedList"
        self.cache_cnt = 0
        self.head = CacheObject(None, None, None, None)
        self.tail = CacheObject(None, None, None, None)
        self.head.next = self.tail
        self.tail.pre = self.head

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

    def _insert_front(self, co: CacheObject) -> None:
        co.pre = self.head
        co.next = self.head.next
        co.pre.next = co
        co.next.pre = co

    def inform_use(self, tntid, key) -> None:
        """ called when a key is read or updated; only update last use ts """
        ptr = self._get_ptr(tntid, key)
        assert ptr is not None
        ptr.last_used = time.time()
        ptr.pre.next = ptr.next
        ptr.next.pre = ptr.pre
        self._insert_front(ptr)

    def inform_set(self, tntid, key, ttl) -> None:
        """ called when a new key is brought into cache """
        self.cache_cnt += 1
        co = CacheObject(tntid, key, time.time(), ttl)
        self._insert_front(co)

    def arbit_evict(self, tntid, key) -> Tuple[str, str]:
        """ called when a new key should be brought in and cache is full """
        self.cache_cnt -= 1
        last = self.tail.pre
        assert last.tntid is not None and last.key is not None
        last.pre.next = last.next
        last.next.pre = last.pre
        evict_tntid = last.tntid
        evict_key = last.key
        del last
        return evict_tntid, evict_key

    def oldest_time(self) -> Optional[float]:
        co = self.tail.pre
        if co is self.head:
            return None
        return co.last_used
