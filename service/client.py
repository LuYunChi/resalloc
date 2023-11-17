import threading
import redis

from service.allocator.abstract import Allocator


class CacheClient:
    def __init__(self, allocator: Allocator) -> None:
        self.redis = redis.Redis(
            host='localhost', port=6379, decode_responses=True)
        self.allocator = allocator
        self.lock = threading.Lock()

    def reset(self):
        self.redis.flushdb()

    def handle(self, tntid, key, val, iswrite, ttl) -> bool:
        """ handle READ if val is None, else handle WRITE; return hit """
        self.lock.acquire()
        hit = self.allocator.key_in_cache(tntid, key)
        if hit:
            if iswrite:
                self.allocator.inform_use(tntid, key)
                self._write_redis(tntid, key, val)
            else:
                self.allocator.inform_use(tntid, key)
                self._read_redis(tntid, key)
        else:
            if self.allocator.cache_isfull():
                evict_tntid, evict_key = self.allocator.arbit_evict(tntid, key)
                self._evict_redis(evict_tntid, evict_key)
            self.allocator.inform_set(tntid, key, ttl)
            self._write_redis(tntid, key, val)
        self.lock.release()
        return hit

    def _read_redis(self, tntid, key) -> None:
        wk = self._wrap_key(tntid, key)
        self.redis.get(wk)

    def _write_redis(self, tntid, key, val) -> None:
        wk = self._wrap_key(tntid, key)
        self.redis.set(wk, val)

    def _evict_redis(self, tntid, key) -> None:
        wk = self._wrap_key(tntid, key)
        self.redis.delete(wk)

    def _wrap_key(self, tntid, key) -> str:
        return f"{tntid}:{key}"
