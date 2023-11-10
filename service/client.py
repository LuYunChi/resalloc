import redis

from service.allocator.abstract import Allocator


class CacheClient:
    def __init__(self, allocator: Allocator) -> None:
        self.redis = redis.Redis(
            host='localhost', port=6379, decode_responses=True)
        self.allocator = allocator

    def reset(self):
        self.redis.flushdb()

    def get(self, tntid, key) -> bool:
        wk = self.wrap_key(tntid, key)
        result = self.redis.get(wk) is not None
        self.allocator.inform_get(tntid, wk, result)
        return result

    def set(self, tntid, key) -> None:
        wk = self.wrap_key(tntid, key)
        evict_key = self.allocator.inform_set(tntid, wk)
        if evict_key is not None:
            self.redis.delete(evict_key)
        self.redis.set(wk, "default_value")

    def wrap_key(self, tntid, key) -> str:
        return f"{tntid}:{key}"
